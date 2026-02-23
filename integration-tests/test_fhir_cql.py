"""CQL evaluation endpoint integration tests.

Tests the POST /{dataset_id}/$cql endpoint end-to-end with pre-compiled
ELM JSON fixtures.  No external CQL translation service is needed.
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


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# HTTP client (duplicated from test_fhir_standalone to avoid cross-import)
# ---------------------------------------------------------------------------

class FhirClient:
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _seed_conditions(client, dataset_id, patient_ids):
    """Create conditions linked to patients, return their ids."""
    p1_id, p2_id = patient_ids
    _, c1, _ = client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p1_id}"},
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "73211009", "display": "Diabetes mellitus"}],
        },
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}],
        },
    })
    _, c2, _ = client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p1_id}"},
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "38341003", "display": "Hypertension"}],
        },
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}],
        },
    })
    _, c3, _ = client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p2_id}"},
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "195967001", "display": "Asthma"}],
        },
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "resolved"}],
        },
    })
    return c1["id"], c2["id"], c3["id"]


# ---------------------------------------------------------------------------
# ELM builders
# ---------------------------------------------------------------------------

def _elm_library(*defs):
    """Wrap expression defs in a minimal ELM library structure."""
    all_defs = [
        # Patient context definition (always first, skipped by compiler)
        {
            "name": "Patient",
            "context": "Patient",
            "expression": {
                "type": "Retrieve",
                "dataType": "{http://hl7.org/fhir}Patient",
            },
        },
    ] + list(defs)
    return {
        "identifier": {"id": "Test", "version": "1.0.0"},
        "statements": {"def": all_defs},
    }


def _retrieve(resource_type="Patient"):
    return {"type": "Retrieve", "dataType": f"{{http://hl7.org/fhir}}{resource_type}"}


def _property(path, scope=None):
    p = {"type": "Property", "path": path}
    if scope:
        p["scope"] = scope
    return p


def _literal(value, value_type="String"):
    return {
        "type": "Literal",
        "valueType": f"{{urn:hl7-org:elm-types:r1}}{value_type}",
        "value": str(value),
    }


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
# HAPPY PATH
# ===================================================================

def test_cql_simple_retrieve(fhir):
    """Basic [Patient] retrieve returns all patients."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "AllPatients",
        "context": "Patient",
        "expression": _retrieve("Patient"),
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    assert body["resourceType"] == "Parameters"
    assert len(body["parameter"]) >= 1
    param = body["parameter"][0]
    assert param["name"] == "AllPatients"
    # Two seeded patients → should have a "part" list with 2 entries
    assert "part" in param
    assert len(param["part"]) == 2


def test_cql_query_with_where(fhir):
    """[Patient] P where P.gender = 'male' returns only male patients."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "MalePatients",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "Equal",
                "operand": [
                    _property("gender", scope="P"),
                    _literal("male"),
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    assert body["resourceType"] == "Parameters"
    param = body["parameter"][0]
    assert param["name"] == "MalePatients"
    # Only one male patient seeded
    assert "valueString" in param  # single-row result


def test_cql_property_access(fhir):
    """Query with return clause extracts a property."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "BirthDates",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "return": {
                "expression": _property("birthDate", scope="P"),
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "BirthDates"
    # Two patients → two birthDate values
    assert "part" in param
    assert len(param["part"]) == 2
    values = [p["valueString"] for p in param["part"]]
    assert any("1990" in v for v in values)
    assert any("2010" in v for v in values)


def test_cql_count_aggregate(fhir):
    """Count([Patient]) returns the correct count."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "PatientCount",
        "context": "Patient",
        "expression": {
            "type": "Count",
            "source": _retrieve("Patient"),
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "PatientCount"
    assert "2" in param["valueString"]


def test_cql_comparison(fhir):
    """Filter patients by birthDate >= 2000-01-01."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "RecentPatients",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "GreaterOrEqual",
                "operand": [
                    _property("birthDate", scope="P"),
                    _literal("2000-01-01", "Date"),
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "RecentPatients"
    # Only the 2010 patient matches
    assert "valueString" in param  # single row


def test_cql_not_equal(fhir):
    """[Patient] P where P.gender != 'male' returns non-male patients."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "NonMale",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "NotEqual",
                "operand": [
                    _property("gender", scope="P"),
                    _literal("male"),
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "NonMale"
    assert "valueString" in param  # single female patient


def test_cql_multiple_expressions(fhir):
    """Multiple defines: second references first via ExpressionRef."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library(
        {
            "name": "MalePatients",
            "context": "Patient",
            "expression": {
                "type": "Query",
                "source": [{"alias": "P", "expression": _retrieve("Patient")}],
                "where": {
                    "type": "Equal",
                    "operand": [
                        _property("gender", scope="P"),
                        _literal("male"),
                    ],
                },
            },
        },
        {
            "name": "MaleCount",
            "context": "Patient",
            "expression": {
                "type": "Count",
                "source": {"type": "ExpressionRef", "name": "MalePatients"},
            },
        },
    )
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    assert body["resourceType"] == "Parameters"
    # The last expression (MaleCount) is selected — should be count=1
    param = body["parameter"][0]
    assert param["name"] == "MaleCount"
    assert "1" in param["valueString"]


# ===================================================================
# CONDITION RESOURCE TYPE
# ===================================================================

def test_cql_condition_retrieve(fhir):
    """[Condition] retrieves all conditions in the dataset."""
    did = _create_dataset(fhir)
    pids = _seed_patients(fhir, did)
    _seed_conditions(fhir, did, pids)

    elm = _elm_library({
        "name": "AllConditions",
        "context": "Patient",
        "expression": _retrieve("Condition"),
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    assert body["resourceType"] == "Parameters"
    param = body["parameter"][0]
    assert param["name"] == "AllConditions"
    assert "part" in param
    assert len(param["part"]) == 3  # three seeded conditions


def test_cql_condition_count(fhir):
    """Count([Condition]) returns the correct count."""
    did = _create_dataset(fhir)
    pids = _seed_patients(fhir, did)
    _seed_conditions(fhir, did, pids)

    elm = _elm_library({
        "name": "ConditionCount",
        "context": "Patient",
        "expression": {
            "type": "Count",
            "source": _retrieve("Condition"),
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "ConditionCount"
    assert "3" in param["valueString"]


# ===================================================================
# AND / OR / NOT OPERATORS
# ===================================================================

def test_cql_and_operator(fhir):
    """P.gender = 'male' AND P.birthDate < '2000-01-01' returns one patient."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "MaleAndOld",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "And",
                "operand": [
                    {
                        "type": "Equal",
                        "operand": [
                            _property("gender", scope="P"),
                            _literal("male"),
                        ],
                    },
                    {
                        "type": "Less",
                        "operand": [
                            _property("birthDate", scope="P"),
                            _literal("2000-01-01", "Date"),
                        ],
                    },
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "MaleAndOld"
    # Male patient born 1990 matches both conditions
    assert "valueString" in param  # single row


def test_cql_and_no_match(fhir):
    """P.gender = 'female' AND P.birthDate < '2000-01-01' returns empty."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "FemaleAndOld",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "And",
                "operand": [
                    {
                        "type": "Equal",
                        "operand": [
                            _property("gender", scope="P"),
                            _literal("female"),
                        ],
                    },
                    {
                        "type": "Less",
                        "operand": [
                            _property("birthDate", scope="P"),
                            _literal("2000-01-01", "Date"),
                        ],
                    },
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    # Female patient was born 2010, no match
    assert body["parameter"] == []


def test_cql_or_operator(fhir):
    """P.gender = 'male' OR P.birthDate > '2005-01-01' returns both patients."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "MaleOrYoung",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "Or",
                "operand": [
                    {
                        "type": "Equal",
                        "operand": [
                            _property("gender", scope="P"),
                            _literal("male"),
                        ],
                    },
                    {
                        "type": "Greater",
                        "operand": [
                            _property("birthDate", scope="P"),
                            _literal("2005-01-01", "Date"),
                        ],
                    },
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "MaleOrYoung"
    # Male matches first condition, female (2010) matches second
    assert "part" in param
    assert len(param["part"]) == 2


def test_cql_not_operator(fhir):
    """NOT (P.gender = 'male') returns only female patients."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "NotMale",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "Not",
                "operand": {
                    "type": "Equal",
                    "operand": [
                        _property("gender", scope="P"),
                        _literal("male"),
                    ],
                },
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "NotMale"
    assert "valueString" in param  # single female patient


# ===================================================================
# ADDITIONAL OPERATORS
# ===================================================================

def test_cql_isnull(fhir):
    """IsNull on a property that exists returns patients where it is null."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    # Both patients have birthDate set, so IsNull should return empty
    elm = _elm_library({
        "name": "NoBirthDate",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "where": {
                "type": "IsNull",
                "operand": _property("birthDate", scope="P"),
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    assert body["parameter"] == []


def test_cql_coalesce(fhir):
    """Coalesce returns the first non-null value."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "CoalesceTest",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "return": {
                "expression": {
                    "type": "Coalesce",
                    "operand": [
                        _property("deceasedDateTime", scope="P"),
                        _literal("alive"),
                    ],
                },
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "CoalesceTest"
    # Both patients are alive (no deceasedDateTime), so coalesce returns "alive"
    assert "part" in param
    values = [p["valueString"] for p in param["part"]]
    assert all("alive" in v for v in values)


def test_cql_if_expression(fhir):
    """If-then-else classifies patients by gender."""
    did = _create_dataset(fhir)
    _seed_patients(fhir, did)

    elm = _elm_library({
        "name": "GenderLabel",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve("Patient")}],
            "return": {
                "expression": {
                    "type": "If",
                    "condition": {
                        "type": "Equal",
                        "operand": [
                            _property("gender", scope="P"),
                            _literal("male"),
                        ],
                    },
                    "then": _literal("M"),
                    "else": _literal("F"),
                },
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "GenderLabel"
    assert "part" in param
    values = sorted([p["valueString"] for p in param["part"]])
    assert values == ['"F"', '"M"']


def test_cql_condition_filter_by_clinical_status(fhir):
    """Filter conditions by clinicalStatus coding code = 'active'."""
    did = _create_dataset(fhir)
    pids = _seed_patients(fhir, did)
    _seed_conditions(fhir, did, pids)

    elm = _elm_library({
        "name": "ActiveConditions",
        "context": "Patient",
        "expression": {
            "type": "Query",
            "source": [{"alias": "C", "expression": _retrieve("Condition")}],
            "where": {
                "type": "Equal",
                "operand": [
                    _property("clinicalStatus.coding[0].code", scope="C"),
                    _literal("active"),
                ],
            },
        },
    })
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    param = body["parameter"][0]
    assert param["name"] == "ActiveConditions"
    # Two conditions are active (diabetes, hypertension), one is resolved (asthma)
    assert "part" in param
    assert len(param["part"]) == 2


def test_cql_condition_and_patient_combined(fhir):
    """Count conditions AND count patients in same library."""
    did = _create_dataset(fhir)
    pids = _seed_patients(fhir, did)
    _seed_conditions(fhir, did, pids)

    elm = _elm_library(
        {
            "name": "PatientCount",
            "context": "Patient",
            "expression": {
                "type": "Count",
                "source": _retrieve("Patient"),
            },
        },
        {
            "name": "ConditionCount",
            "context": "Patient",
            "expression": {
                "type": "Count",
                "source": _retrieve("Condition"),
            },
        },
    )
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 200
    # Last expression (ConditionCount) is the result
    param = body["parameter"][0]
    assert param["name"] == "ConditionCount"
    assert "3" in param["valueString"]


# ===================================================================
# ERROR CASES
# ===================================================================

def test_cql_invalid_elm(fhir):
    """Invalid ELM structure returns 400."""
    did = _create_dataset(fhir)
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": {"not": "valid"}})
    assert status == 400


def test_cql_missing_library(fhir):
    """Request without 'library' or 'libraryUrl' returns 400."""
    did = _create_dataset(fhir)
    status, body, _ = fhir.post(f"/{did}/$cql", {"foo": "bar"})
    assert status == 400


def test_cql_nonexistent_dataset(fhir):
    """Valid ELM against a nonexistent dataset returns 500."""
    elm = _elm_library({
        "name": "Test",
        "context": "Patient",
        "expression": _retrieve("Patient"),
    })
    status, body, _ = fhir.post("/nonexistent-ds-abc/$cql", {"library": elm})
    assert status == 500


def test_cql_empty_statements(fhir):
    """ELM with no statements field returns 400."""
    did = _create_dataset(fhir)
    elm = {"identifier": {"id": "Empty", "version": "1.0.0"}}
    status, body, _ = fhir.post(f"/{did}/$cql", {"library": elm})
    assert status == 400
