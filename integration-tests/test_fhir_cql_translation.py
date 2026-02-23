"""End-to-end CQL text translation tests.

Tests the POST /{dataset_id}/$cql endpoint with raw CQL text (the "cql" field),
which requires both the FHIR and cql2elm extensions loaded in the same DuckDB instance.
"""

import json
import os
import socket
import time
import uuid

import pytest

from conftest import CQL2ELM_EXT, FHIR_EXT, Node, alloc_ports

os.environ.setdefault("FHIR_POOL_SIZE", "1")


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FhirClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, path, data=None, headers=None):
        import urllib.request
        import urllib.error
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


def _uid():
    return f"t-{uuid.uuid4().hex[:8]}"


def _create_dataset(client, dataset_id=None):
    did = dataset_id or _uid()
    status, body, _ = client.post("/datasets", {"id": did, "name": f"CQL {did}"})
    assert status == 201, f"create_dataset failed ({status}): {body}"
    return did


def _seed_patients(client, dataset_id):
    """Create two patients for CQL tests, return their ids."""
    _, p1, _ = client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "gender": "male",
        "birthDate": "1990-01-15",
        "name": [{"family": "Adams", "given": ["John"]}],
    })
    _, p2, _ = client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "gender": "female",
        "birthDate": "2010-06-01",
        "name": [{"family": "Baker", "given": ["Jane"]}],
    })
    return p1["id"], p2["id"]


@pytest.fixture(scope="module")
def fhir_with_cql2elm():
    """Start FHIR server with both FHIR and cql2elm extensions loaded."""
    gp, fp, pp = alloc_ports()
    node = Node([CQL2ELM_EXT, FHIR_EXT], gp, fp, pp)

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


def test_cql_text_to_results(fhir_with_cql2elm):
    """POST with raw CQL text translates and evaluates, returning Parameters."""
    client = fhir_with_cql2elm
    did = _create_dataset(client)
    _seed_patients(client, did)

    cql_text = (
        "library InlineTest version '1.0.0' "
        "using FHIR version '4.0.1' "
        "context Patient "
        "define AllPatients: [Patient]"
    )
    status, body, _ = client.post(f"/{did}/$cql", {"cql": cql_text})
    assert status == 200, f"Expected 200, got {status}: {body}"
    assert body["resourceType"] == "Parameters"
    assert len(body["parameter"]) >= 1
    param = body["parameter"][0]
    assert param["name"] == "AllPatients"


def test_cql_text_with_filter(fhir_with_cql2elm):
    """CQL text with a where clause returns filtered results."""
    client = fhir_with_cql2elm
    did = _create_dataset(client)
    _seed_patients(client, did)

    cql_text = (
        "library FilterTest version '1.0.0' "
        "using FHIR version '4.0.1' "
        "context Patient "
        "define MalePatients: [Patient] P where P.gender = 'male'"
    )
    status, body, _ = client.post(f"/{did}/$cql", {"cql": cql_text})
    assert status == 200, f"Expected 200, got {status}: {body}"
    assert body["resourceType"] == "Parameters"
    param = body["parameter"][0]
    assert param["name"] == "MalePatients"
    # Only one male patient
    assert "valueString" in param


def test_cql_text_missing_extension_message(fhir_with_cql2elm):
    """Passing invalid CQL text returns a meaningful error."""
    client = fhir_with_cql2elm
    did = _create_dataset(client)

    status, body, _ = client.post(f"/{did}/$cql", {"cql": "not valid cql"})
    # Should get 400 because translation will fail
    assert status == 400
