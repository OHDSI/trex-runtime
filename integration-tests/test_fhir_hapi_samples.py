"""FHIR R4 Server tests using real-world sample data from HAPI FHIR.

Fetches live resources from the public HAPI FHIR R4 server
(https://hapi.fhir.org/baseR4/) and validates that our implementation
correctly handles them: create, read-back, update, search, bundle
ingest, and cross-resource references.

Requires network access to hapi.fhir.org.  Skipped automatically if
the server is unreachable.
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

os.environ.setdefault("FHIR_POOL_SIZE", "1")

HAPI_BASE = "https://hapi.fhir.org/baseR4"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _uid():
    return f"h-{uuid.uuid4().hex[:8]}"


class FhirClient:
    """Thin stdlib HTTP client."""

    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, path, data=None, headers=None):
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
            body = text
        hdrs = {k.lower(): v for k, v in headers.items()}
        return status, body, hdrs

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, data, **kw):
        return self.request("POST", path, data, **kw)

    def put(self, path, data, **kw):
        return self.request("PUT", path, data, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)


def _fetch_hapi(resource_type, count=5):
    """Fetch resources from HAPI.  Returns list of resource dicts (no id/meta)."""
    url = f"{HAPI_BASE}/{resource_type}?_count={count}&_format=json"
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/fhir+json")
    with urllib.request.urlopen(req, timeout=20) as resp:
        bundle = json.loads(resp.read().decode("utf-8"))
    resources = []
    for entry in bundle.get("entry", []):
        r = entry.get("resource", {})
        # Strip server-assigned fields so our server assigns its own
        r.pop("id", None)
        r.pop("meta", None)
        r.pop("text", None)
        resources.append(r)
    return resources


def _create_dataset(client, dataset_id=None):
    did = dataset_id or _uid()
    status, body, _ = client.post("/datasets", {"id": did, "name": f"HAPI test {did}"})
    assert status == 201, f"create_dataset failed ({status}): {body}"
    return did


# ---------------------------------------------------------------------------
# Check HAPI accessibility — skip entire module if unreachable
# ---------------------------------------------------------------------------

def _hapi_reachable():
    try:
        req = urllib.request.Request(f"{HAPI_BASE}/metadata?_format=json")
        with urllib.request.urlopen(req, timeout=10):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _hapi_reachable(),
    reason="HAPI FHIR public server unreachable",
)


# ---------------------------------------------------------------------------
# Module-scoped fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fhir():
    gp, fp, pp = alloc_ports()
    node = Node([FHIR_EXT], gp, fp, pp)

    fhir_port = _free_port()
    result = node.execute(f"SELECT trex_fhir_start('127.0.0.1', {fhir_port})")
    assert len(result) == 1 and "Started" in result[0][0], f"trex_fhir_start: {result}"

    client = FhirClient(f"http://127.0.0.1:{fhir_port}")

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
# PATIENT — real HAPI data
# ===================================================================

class TestHapiPatients:
    """Load real Patient resources from HAPI and exercise CRUD."""

    def test_create_and_read_hapi_patients(self, fhir):
        """POST 5 real Patient resources, GET each one back."""
        patients = _fetch_hapi("Patient", count=5)
        assert len(patients) >= 1, "HAPI returned no Patient resources"

        did = _create_dataset(fhir)
        created_ids = []

        for patient in patients:
            status, body, hdrs = fhir.post(f"/{did}/Patient", patient)
            assert status == 201, f"Create failed: {body}"
            assert body["resourceType"] == "Patient"
            assert "id" in body
            assert body["meta"]["versionId"] == "1"
            created_ids.append(body["id"])

        # Read each one back
        for pid in created_ids:
            status, body, _ = fhir.get(f"/{did}/Patient/{pid}")
            assert status == 200
            assert body["id"] == pid
            assert body["resourceType"] == "Patient"

    def test_search_hapi_patients(self, fhir):
        """Search returns the patients we just created."""
        patients = _fetch_hapi("Patient", count=3)
        did = _create_dataset(fhir)

        for p in patients:
            s, _, _ = fhir.post(f"/{did}/Patient", p)
            assert s == 201

        status, body, _ = fhir.get(f"/{did}/Patient")
        assert status == 200
        assert body["resourceType"] == "Bundle"
        assert body["type"] == "searchset"
        assert body["total"] == len(patients)

    def test_update_hapi_patient(self, fhir):
        """Update a HAPI patient and verify version increments."""
        patients = _fetch_hapi("Patient", count=1)
        did = _create_dataset(fhir)

        _, created, _ = fhir.post(f"/{did}/Patient", patients[0])
        pid = created["id"]

        # Modify the resource
        updated = created.copy()
        updated["gender"] = "other"
        status, body, _ = fhir.put(f"/{did}/Patient/{pid}", updated)
        assert status == 200
        assert body["meta"]["versionId"] == "2"
        assert body["gender"] == "other"

    def test_delete_hapi_patient(self, fhir):
        """Delete a HAPI patient and verify 410."""
        patients = _fetch_hapi("Patient", count=1)
        did = _create_dataset(fhir)

        _, created, _ = fhir.post(f"/{did}/Patient", patients[0])
        pid = created["id"]

        status, _, _ = fhir.delete(f"/{did}/Patient/{pid}")
        assert status == 204

        status, body, _ = fhir.get(f"/{did}/Patient/{pid}")
        assert status == 410


# ===================================================================
# OBSERVATION — real HAPI data
# ===================================================================

class TestHapiObservations:
    """Load real Observation resources from HAPI."""

    def test_create_and_read_observations(self, fhir):
        """POST 3 real Observations, read each back, verify fields preserved."""
        observations = _fetch_hapi("Observation", count=3)
        assert len(observations) >= 1

        did = _create_dataset(fhir)
        created = []

        for obs in observations:
            status, body, _ = fhir.post(f"/{did}/Observation", obs)
            assert status == 201, f"Create failed: {body}"
            assert body["resourceType"] == "Observation"
            created.append(body)

        # Read back and verify key fields preserved
        for c in created:
            status, body, _ = fhir.get(f"/{did}/Observation/{c['id']}")
            assert status == 200
            assert body["status"] == c["status"]
            assert body.get("code") == c.get("code")

    def test_search_observations_by_status(self, fhir):
        """Search Observations by status token."""
        observations = _fetch_hapi("Observation", count=3)
        did = _create_dataset(fhir)

        for obs in observations:
            fhir.post(f"/{did}/Observation", obs)

        status, body, _ = fhir.get(f"/{did}/Observation?status=final")
        assert status == 200
        assert body["resourceType"] == "Bundle"
        # All HAPI observations we fetched are status=final
        for entry in body.get("entry", []):
            assert entry["resource"]["status"] == "final"


# ===================================================================
# CONDITION — real HAPI data
# ===================================================================

class TestHapiConditions:
    """Load real Condition resources from HAPI."""

    def test_create_and_read_conditions(self, fhir):
        """POST Conditions with coded fields (SNOMED), read them back."""
        conditions = _fetch_hapi("Condition", count=3)
        assert len(conditions) >= 1

        did = _create_dataset(fhir)

        for cond in conditions:
            status, body, _ = fhir.post(f"/{did}/Condition", cond)
            assert status == 201, f"Create failed: {body}"
            cid = body["id"]

            # Read back
            status, body, _ = fhir.get(f"/{did}/Condition/{cid}")
            assert status == 200
            assert body["resourceType"] == "Condition"
            # Verify coded fields survived round-trip
            if "code" in cond:
                assert body["code"] == cond["code"]


# ===================================================================
# ENCOUNTER — real HAPI data
# ===================================================================

class TestHapiEncounters:
    """Load real Encounter resources from HAPI."""

    def test_create_and_read_encounters(self, fhir):
        encounters = _fetch_hapi("Encounter", count=3)
        assert len(encounters) >= 1

        did = _create_dataset(fhir)

        for enc in encounters:
            status, body, _ = fhir.post(f"/{did}/Encounter", enc)
            assert status == 201, f"Create failed ({status}): {body}"
            eid = body["id"]

            status, body, _ = fhir.get(f"/{did}/Encounter/{eid}")
            assert status == 200
            assert body["resourceType"] == "Encounter"


# ===================================================================
# TRANSACTION BUNDLE — mixed resource types from HAPI
# ===================================================================

class TestHapiBundles:
    """Build a transaction bundle from HAPI resources."""

    def test_transaction_with_mixed_hapi_resources(self, fhir):
        """Build a transaction bundle with Patient + Observation from HAPI."""
        patients = _fetch_hapi("Patient", count=1)
        observations = _fetch_hapi("Observation", count=2)

        did = _create_dataset(fhir)

        patient_urn = f"urn:uuid:{uuid.uuid4()}"
        entries = []

        # Patient entry
        entries.append({
            "fullUrl": patient_urn,
            "resource": patients[0],
            "request": {"method": "POST", "url": "Patient"},
        })

        # Observation entries referencing the patient
        for obs in observations:
            obs["subject"] = {"reference": patient_urn}
            entries.append({
                "resource": obs,
                "request": {"method": "POST", "url": "Observation"},
            })

        bundle = {"resourceType": "Bundle", "type": "transaction", "entry": entries}
        status, body, _ = fhir.post(f"/{did}", bundle)
        assert status == 200, f"Transaction failed: {body}"
        assert body["type"] == "transaction-response"
        assert len(body["entry"]) == len(entries)

        # All entries should be 201 Created
        for i, entry in enumerate(body["entry"]):
            assert "201" in entry["response"]["status"], (
                f"Entry {i} failed: {entry['response']}"
            )

    def test_batch_with_hapi_conditions(self, fhir):
        """Batch-load Condition resources from HAPI."""
        conditions = _fetch_hapi("Condition", count=3)
        did = _create_dataset(fhir)

        entries = []
        for cond in conditions:
            entries.append({
                "resource": cond,
                "request": {"method": "POST", "url": "Condition"},
            })

        bundle = {"resourceType": "Bundle", "type": "batch", "entry": entries}
        status, body, _ = fhir.post(f"/{did}", bundle)
        assert status == 200
        assert body["type"] == "batch-response"

        created_count = sum(
            1 for e in body["entry"] if "201" in e["response"]["status"]
        )
        assert created_count == len(conditions)


# ===================================================================
# HISTORY — with HAPI resource
# ===================================================================

class TestHapiHistory:
    """Version history with real HAPI data."""

    def test_history_after_update_with_hapi_patient(self, fhir):
        """Create HAPI patient, update twice, verify 3 versions in history."""
        patients = _fetch_hapi("Patient", count=1)
        did = _create_dataset(fhir)

        s, created, _ = fhir.post(f"/{did}/Patient", patients[0])
        assert s == 201
        pid = created["id"]
        assert created["meta"]["versionId"] == "1"

        # Update 1
        v2 = created.copy()
        v2["gender"] = "other"
        s, body2, _ = fhir.put(f"/{did}/Patient/{pid}", v2)
        assert s == 200, f"Update 1 failed: {body2}"
        assert body2["meta"]["versionId"] == "2"

        # Update 2
        v3 = body2.copy()
        v3["birthDate"] = "2000-01-01"
        s, body3, _ = fhir.put(f"/{did}/Patient/{pid}", v3)
        assert s == 200, f"Update 2 failed: {body3}"
        assert body3["meta"]["versionId"] == "3"

        status, body, _ = fhir.get(f"/{did}/Patient/{pid}/_history")
        assert status == 200
        assert body["type"] == "history"
        assert body["total"] == 3, f"Expected 3 history entries, got {body['total']}. Entries: {json.dumps(body['entry'], indent=2)[:1000]}"


# ===================================================================
# EDGE CASES — stress test with diverse HAPI data
# ===================================================================

class TestHapiEdgeCases:
    """Edge cases using real-world HAPI data."""

    def test_unicode_in_patient_names(self, fhir):
        """HAPI patients often have unicode names (accents, CJK, etc.)."""
        patients = _fetch_hapi("Patient", count=5)
        did = _create_dataset(fhir)

        for p in patients:
            status, body, _ = fhir.post(f"/{did}/Patient", p)
            assert status == 201
            pid = body["id"]

            # Round-trip: names with unicode should survive
            status, body, _ = fhir.get(f"/{did}/Patient/{pid}")
            assert status == 200
            if "name" in p:
                assert body["name"] == p["name"]

    def test_observation_with_quantity_roundtrip(self, fhir):
        """Verify valueQuantity (decimal, unit, system) survives round-trip."""
        observations = _fetch_hapi("Observation", count=3)
        did = _create_dataset(fhir)

        for obs in observations:
            _, created, _ = fhir.post(f"/{did}/Observation", obs)
            oid = created["id"]

            _, readback, _ = fhir.get(f"/{did}/Observation/{oid}")
            if "valueQuantity" in obs:
                assert readback["valueQuantity"]["value"] == obs["valueQuantity"]["value"]
                assert readback["valueQuantity"]["unit"] == obs["valueQuantity"]["unit"]

    def test_multiple_resource_types_same_dataset(self, fhir):
        """Load Patient, Observation, Condition, Encounter into one dataset."""
        did = _create_dataset(fhir)
        counts = {}

        for rtype in ["Patient", "Observation", "Condition", "Encounter"]:
            resources = _fetch_hapi(rtype, count=2)
            for r in resources:
                s, _, _ = fhir.post(f"/{did}/{rtype}", r)
                assert s == 201, f"Failed to create {rtype}: {s}"
            counts[rtype] = len(resources)

        # Search each type — counts should match
        for rtype, expected in counts.items():
            status, body, _ = fhir.get(f"/{did}/{rtype}")
            assert status == 200
            assert body["total"] == expected, (
                f"{rtype}: expected {expected}, got {body['total']}"
            )
