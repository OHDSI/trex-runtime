"""FHIR R4 Server integration tests.

Tests the HTTP API end-to-end: requests go through axum routing,
middleware, handlers, and hit a real DuckDB database.

A single FHIR server is started per module. Each test creates its own
dataset with a unique name so tests don't interfere with each other.
"""

import json
import os
import socket
import time
import urllib.request
import urllib.error
import uuid

import pytest

from conftest import Node, FHIR_EXT, alloc_ports

# DuckDB cloned connections share the database but have separate transaction
# contexts.  With a multi-worker pool, DDL executed on one worker (CREATE
# SCHEMA / CREATE TABLE) may not be visible to other workers immediately.
# Using a single worker avoids this cross-connection catalog visibility issue
# and is sufficient for integration testing since we are not benchmarking
# concurrency here.
os.environ.setdefault("FHIR_POOL_SIZE", "1")


def _free_port():
    """Ask the OS for a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class FhirClient:
    """Thin HTTP client for FHIR API testing (stdlib only)."""

    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, path, data=None, headers=None):
        """Return (status_code, parsed_body, response_headers)."""
        url = f"{self.base_url}{path}"
        body_bytes = json.dumps(data).encode("utf-8") if data is not None else None
        req = urllib.request.Request(url, data=body_bytes, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return self._parse(resp.status, resp.read(), resp.headers)
        except urllib.error.HTTPError as e:
            return self._parse(e.code, e.read(), e.headers)

    @staticmethod
    def _parse(status, raw_bytes, headers):
        text = raw_bytes.decode("utf-8") if raw_bytes else ""
        try:
            body = json.loads(text) if text.strip() else None
        except json.JSONDecodeError:
            body = text  # plain-text (metrics, etc.)
        # Normalise header keys to lowercase for consistent lookups.
        hdrs = {k.lower(): v for k, v in headers.items()}
        return status, body, hdrs

    # convenience wrappers
    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, data, **kw):
        return self.request("POST", path, data, **kw)

    def put(self, path, data, **kw):
        return self.request("PUT", path, data, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)

    def post_raw(self, path, raw_bytes, content_type="application/json"):
        """Send raw bytes (for invalid-JSON tests)."""
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, data=raw_bytes, method="POST")
        req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return self._parse(resp.status, resp.read(), resp.headers)
        except urllib.error.HTTPError as e:
            return self._parse(e.code, e.read(), e.headers)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid():
    """Short unique dataset id (alphanumeric + hyphen)."""
    return f"t-{uuid.uuid4().hex[:8]}"


def _create_dataset(client, dataset_id=None, name=None):
    """Create a dataset and return its id (asserts 201)."""
    did = dataset_id or _uid()
    status, body, _ = client.post("/datasets", {"id": did, "name": name or f"DS {did}"})
    assert status == 201, f"create_dataset failed ({status}): {body}"
    return did


def _create_patient(client, dataset_id, family="Doe", given="John", gender="male"):
    """POST a Patient, return (status, body, headers)."""
    return client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "name": [{"family": family, "given": [given]}],
        "gender": gender,
    })


# ---------------------------------------------------------------------------
# Module-scoped fixture: one FHIR server for all tests in this file
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fhir():
    """Start the FHIR extension, launch the HTTP server, yield an FhirClient."""
    gp, fp, pp = alloc_ports()
    node = Node([FHIR_EXT], gp, fp, pp)

    fhir_port = _free_port()
    result = node.execute(f"SELECT trex_fhir_start('127.0.0.1', {fhir_port})")
    assert len(result) == 1 and "Started" in result[0][0], f"trex_fhir_start: {result}"

    client = FhirClient(f"http://127.0.0.1:{fhir_port}")

    # Poll /health until server is ready (definition loading can take seconds)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            s, _, _ = client.get("/health")
            if s == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    else:
        node.close()
        pytest.fail("FHIR server did not become healthy within 30 s")

    yield client

    try:
        node.execute(f"SELECT trex_fhir_stop('127.0.0.1', {fhir_port})")
    except Exception:
        pass
    node.close()


# ===================================================================
# HEALTH
# ===================================================================

def test_health_check(fhir):
    """GET /health returns 200 with status=healthy."""
    status, body, _ = fhir.get("/health")
    assert status == 200
    assert body["status"] == "healthy"
    assert body["database"] == "connected"


def test_metrics_prometheus_format(fhir):
    """GET /metrics returns Prometheus text exposition."""
    status, body, hdrs = fhir.get("/metrics")
    assert status == 200
    assert "text/plain" in hdrs.get("content-type", "")
    assert "fhir_requests_total" in body
    assert "fhir_errors_total" in body


# ===================================================================
# DATASETS
# ===================================================================

def test_create_dataset(fhir):
    did = _uid()
    status, body, _ = fhir.post("/datasets", {"id": did, "name": "Create Test"})
    assert status == 201
    assert body["id"] == did
    assert body["status"] == "active"
    assert body["resource_count"] > 0


def test_list_datasets_includes_new(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get("/datasets")
    assert status == 200
    assert isinstance(body, list)
    assert did in [d["id"] for d in body]


def test_get_dataset(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/datasets/{did}")
    assert status == 200
    assert body["id"] == did


def test_delete_dataset(fhir):
    did = _create_dataset(fhir)
    status, _, _ = fhir.delete(f"/datasets/{did}")
    assert status == 204
    # confirm gone
    status, _, _ = fhir.get(f"/datasets/{did}")
    assert status == 404


def test_create_dataset_invalid_id(fhir):
    status, body, _ = fhir.post("/datasets", {"id": "has spaces!", "name": "Bad"})
    assert status == 400


def test_create_dataset_duplicate(fhir):
    did = _create_dataset(fhir)
    status, _, _ = fhir.post("/datasets", {"id": did, "name": "Dup"})
    assert status in (400, 409)


def test_get_dataset_not_found(fhir):
    status, body, _ = fhir.get("/datasets/nonexistent-ds-xyz")
    assert status == 404
    assert body["resourceType"] == "OperationOutcome"


def test_delete_dataset_not_found(fhir):
    status, _, _ = fhir.delete("/datasets/nonexistent-ds-xyz")
    assert status == 404


# ===================================================================
# RESOURCE CRUD
# ===================================================================

def test_create_patient(fhir):
    did = _create_dataset(fhir)
    status, body, hdrs = _create_patient(fhir, did)
    assert status == 201
    assert body["resourceType"] == "Patient"
    assert "id" in body
    assert body["meta"]["versionId"] == "1"
    assert "location" in hdrs
    assert "etag" in hdrs


def test_read_patient(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]

    status, body, hdrs = fhir.get(f"/{did}/Patient/{pid}")
    assert status == 200
    assert body["id"] == pid
    assert body["resourceType"] == "Patient"


def test_update_patient(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]

    status, body, _ = fhir.put(f"/{did}/Patient/{pid}", {
        "resourceType": "Patient",
        "id": pid,
        "name": [{"family": "Smith", "given": ["Jane"]}],
        "gender": "female",
    })
    assert status == 200
    assert body["meta"]["versionId"] == "2"
    assert body["name"][0]["family"] == "Smith"


def test_delete_patient(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]

    status, _, _ = fhir.delete(f"/{did}/Patient/{pid}")
    assert status == 204


def test_read_deleted_patient_returns_410(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]
    fhir.delete(f"/{did}/Patient/{pid}")

    status, body, _ = fhir.get(f"/{did}/Patient/{pid}")
    assert status == 410
    assert body["resourceType"] == "OperationOutcome"


def test_upsert_via_put(fhir):
    did = _create_dataset(fhir)
    new_id = str(uuid.uuid4())
    status, body, _ = fhir.put(f"/{did}/Patient/{new_id}", {
        "resourceType": "Patient",
        "id": new_id,
        "name": [{"family": "Upsert"}],
    })
    assert status == 201
    assert body["id"] == new_id


def test_create_wrong_resource_type(fhir):
    """POST Observation body to Patient endpoint -> 400."""
    did = _create_dataset(fhir)
    status, _, _ = fhir.post(f"/{did}/Patient", {
        "resourceType": "Observation",
        "code": {"text": "wrong"},
    })
    assert status == 400


def test_read_nonexistent_resource(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/Patient/does-not-exist")
    assert status == 404


# ===================================================================
# SEARCH
# ===================================================================

def test_search_empty(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/Patient")
    assert status == 200
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "searchset"
    assert body["total"] == 0
    assert body["entry"] == []


def test_search_returns_resources(fhir):
    did = _create_dataset(fhir)
    _create_patient(fhir, did, family="Alpha")
    _create_patient(fhir, did, family="Beta")

    status, body, _ = fhir.get(f"/{did}/Patient")
    assert status == 200
    assert body["total"] == 2
    assert len(body["entry"]) == 2


def test_search_count(fhir):
    did = _create_dataset(fhir)
    for i in range(3):
        _create_patient(fhir, did, family=f"Page{i}")

    status, body, _ = fhir.get(f"/{did}/Patient?_count=2")
    assert status == 200
    assert len(body["entry"]) == 2
    assert body["total"] == 3
    links = {l["relation"]: l["url"] for l in body["link"]}
    assert "next" in links


def test_search_offset(fhir):
    did = _create_dataset(fhir)
    for i in range(3):
        _create_patient(fhir, did, family=f"Off{i}")

    status, body, _ = fhir.get(f"/{did}/Patient?_count=2&_offset=2")
    assert status == 200
    assert len(body["entry"]) == 1
    links = {l["relation"]: l["url"] for l in body["link"]}
    assert "previous" in links


def test_search_by_gender(fhir):
    """Token search parameter on a simple code field."""
    did = _create_dataset(fhir)
    _create_patient(fhir, did, family="A", gender="male")
    _create_patient(fhir, did, family="B", gender="female")
    _create_patient(fhir, did, family="C", gender="male")

    status, body, _ = fhir.get(f"/{did}/Patient?gender=female")
    assert status == 200
    assert body["total"] == 1
    assert body["entry"][0]["resource"]["gender"] == "female"


def test_search_by_family_name(fhir):
    """String search parameter on an array-nested field (Patient.name.family)."""
    did = _create_dataset(fhir)
    _create_patient(fhir, did, family="Smith")
    _create_patient(fhir, did, family="Jones")
    _create_patient(fhir, did, family="Smithson")  # prefix match

    status, body, _ = fhir.get(f"/{did}/Patient?family=smith")
    assert status == 200
    # Default string search is prefix + case-insensitive, so "Smith" and "Smithson" match.
    families = [e["resource"]["name"][0]["family"] for e in body["entry"]]
    assert "Smith" in families
    assert "Smithson" in families
    assert "Jones" not in families


def test_search_excludes_deleted(fhir):
    did = _create_dataset(fhir)
    _, p1, _ = _create_patient(fhir, did, family="Keep")
    _, p2, _ = _create_patient(fhir, did, family="Remove")
    fhir.delete(f"/{did}/Patient/{p2['id']}")

    status, body, _ = fhir.get(f"/{did}/Patient")
    assert status == 200
    assert body["total"] == 1
    assert body["entry"][0]["resource"]["name"][0]["family"] == "Keep"


# ===================================================================
# BUNDLES
# ===================================================================

def _make_bundle(btype, entries):
    return {"resourceType": "Bundle", "type": btype, "entry": entries}


def _entry(resource_type, resource, method="POST", full_url=None):
    e = {"request": {"method": method, "url": resource_type}, "resource": resource}
    if full_url:
        e["fullUrl"] = full_url
    return e


def test_transaction_bundle(fhir):
    did = _create_dataset(fhir)
    bundle = _make_bundle("transaction", [
        _entry("Patient", {"resourceType": "Patient", "name": [{"family": "TxA"}]}),
        _entry("Patient", {"resourceType": "Patient", "name": [{"family": "TxB"}]}),
    ])
    status, body, _ = fhir.post(f"/{did}", bundle)
    assert status == 200
    assert body["type"] == "transaction-response"
    assert len(body["entry"]) == 2
    assert "201" in body["entry"][0]["response"]["status"]


def test_batch_bundle(fhir):
    did = _create_dataset(fhir)
    bundle = _make_bundle("batch", [
        _entry("Patient", {"resourceType": "Patient", "name": [{"family": "BtA"}]}),
        _entry("Patient", {"resourceType": "Patient", "name": [{"family": "BtB"}]}),
    ])
    status, body, _ = fhir.post(f"/{did}", bundle)
    assert status == 200
    assert body["type"] == "batch-response"
    assert len(body["entry"]) == 2


def test_transaction_reference_resolution(fhir):
    did = _create_dataset(fhir)
    patient_urn = f"urn:uuid:{uuid.uuid4()}"
    bundle = _make_bundle("transaction", [
        _entry("Patient", {
            "resourceType": "Patient",
            "name": [{"family": "RefTest"}],
        }, full_url=patient_urn),
        _entry("Observation", {
            "resourceType": "Observation",
            "status": "final",
            "code": {"text": "test"},
            "subject": {"reference": patient_urn},
        }),
    ])
    status, body, _ = fhir.post(f"/{did}", bundle)
    assert status == 200
    assert len(body["entry"]) == 2
    assert "201" in body["entry"][0]["response"]["status"]
    assert "201" in body["entry"][1]["response"]["status"]


def test_batch_partial_failure(fhir):
    """Good Patient + bad ZZZFake in a batch -> first succeeds, second fails."""
    did = _create_dataset(fhir)
    bundle = _make_bundle("batch", [
        _entry("Patient", {"resourceType": "Patient", "name": [{"family": "Good"}]}),
        _entry("ZZZFake", {"resourceType": "ZZZFake"}),
    ])
    status, body, _ = fhir.post(f"/{did}", bundle)
    assert status == 200
    assert body["type"] == "batch-response"
    assert "201" in body["entry"][0]["response"]["status"]
    assert "400" in body["entry"][1]["response"]["status"]


def test_bundle_unsupported_type(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.post(f"/{did}", _make_bundle("document", []))
    assert status == 400


def test_bundle_not_a_bundle(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.post(f"/{did}", {"resourceType": "Patient"})
    assert status == 400


# ===================================================================
# HISTORY
# ===================================================================

def test_history_after_update(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did, family="V1")
    pid = created["id"]

    fhir.put(f"/{did}/Patient/{pid}", {
        "resourceType": "Patient", "id": pid,
        "name": [{"family": "V2"}],
    })

    status, body, _ = fhir.get(f"/{did}/Patient/{pid}/_history")
    assert status == 200
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "history"
    assert body["total"] == 2  # current v2 + history v1


def test_history_after_delete(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did, family="DelHist")
    pid = created["id"]
    fhir.delete(f"/{did}/Patient/{pid}")

    status, body, _ = fhir.get(f"/{did}/Patient/{pid}/_history")
    assert status == 200
    assert body["total"] >= 2
    assert body["entry"][0]["request"]["method"] == "DELETE"


def test_read_specific_version(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did, family="VerA")
    pid = created["id"]

    fhir.put(f"/{did}/Patient/{pid}", {
        "resourceType": "Patient", "id": pid,
        "name": [{"family": "VerB"}],
    })

    # version 1 should still have the original name
    status, body, _ = fhir.get(f"/{did}/Patient/{pid}/_history/1")
    assert status == 200
    assert body["name"][0]["family"] == "VerA"


def test_read_nonexistent_version(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did, family="NoVer")
    pid = created["id"]

    status, _, _ = fhir.get(f"/{did}/Patient/{pid}/_history/999")
    assert status == 404


# ===================================================================
# METADATA
# ===================================================================

def test_capability_statement(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/metadata")
    assert status == 200
    assert body["resourceType"] == "CapabilityStatement"
    assert body["fhirVersion"] == "4.0.1"
    assert "json" in body["format"]


def test_capability_has_patient(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/metadata")
    assert status == 200
    rest = body["rest"][0]
    types = [r["type"] for r in rest["resource"]]
    assert "Patient" in types
    patient = next(r for r in rest["resource"] if r["type"] == "Patient")
    codes = [i["code"] for i in patient["interaction"]]
    assert "read" in codes
    assert "create" in codes


def test_metadata_nonexistent_dataset(fhir):
    status, _, _ = fhir.get("/nonexistent-ds-xyz/metadata")
    assert status == 404


# ===================================================================
# ERROR CASES
# ===================================================================

def test_404_operation_outcome(fhir):
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/Patient/does-not-exist")
    assert status == 404
    assert body["resourceType"] == "OperationOutcome"
    assert body["issue"][0]["severity"] == "error"
    assert body["issue"][0]["code"] == "not-found"


def test_400_operation_outcome(fhir):
    """Missing resourceType triggers validation -> 400 OperationOutcome."""
    did = _create_dataset(fhir)
    status, body, _ = fhir.post(f"/{did}/Patient", {"no_rt": True})
    assert status == 400
    assert body["resourceType"] == "OperationOutcome"


def test_410_operation_outcome(fhir):
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]
    fhir.delete(f"/{did}/Patient/{pid}")

    status, body, _ = fhir.get(f"/{did}/Patient/{pid}")
    assert status == 410
    assert body["resourceType"] == "OperationOutcome"
    assert body["issue"][0]["code"] == "deleted"


def test_invalid_json_returns_error(fhir):
    did = _create_dataset(fhir)
    status, _, _ = fhir.post_raw(f"/{did}/Patient", b"not valid json{{{")
    assert status in (400, 422)


def test_fhir_content_type(fhir):
    """FHIR dataset-scoped endpoints return application/fhir+json."""
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]
    _, _, hdrs = fhir.get(f"/{did}/Patient/{pid}")
    ct = hdrs.get("content-type", "")
    assert "fhir+json" in ct


def test_health_content_type(fhir):
    """Health endpoint returns plain application/json (not fhir+json)."""
    _, _, hdrs = fhir.get("/health")
    ct = hdrs.get("content-type", "")
    assert "application/json" in ct
    assert "fhir" not in ct


def test_version_conflict(fhir):
    """If-Match with stale version -> 409 Conflict."""
    did = _create_dataset(fhir)
    _, created, _ = _create_patient(fhir, did)
    pid = created["id"]

    status, _, _ = fhir.put(
        f"/{did}/Patient/{pid}",
        {"resourceType": "Patient", "id": pid, "name": [{"family": "Conflict"}]},
        headers={"If-Match": 'W/"999"'},
    )
    assert status == 409
