"""MII-style $evaluate-measure endpoint integration tests.

Tests the POST /{dataset_id}/Measure/$evaluate-measure endpoint using the
standard FHIR workflow: seed data -> POST Library -> POST Measure -> evaluate.
"""

import base64
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


def _b64(text):
    """Base64-encode a string."""
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# ---------------------------------------------------------------------------
# ELM construction helpers
# ---------------------------------------------------------------------------

def _elm_library_with_codes(identifier, codesystems, codes, *defs):
    """Build an ELM library JSON with codesystems, codes, and expression defs."""
    lib = {
        "identifier": identifier,
        "statements": {
            "def": [
                {
                    "name": "Patient",
                    "context": "Patient",
                    "expression": {
                        "type": "Retrieve",
                        "dataType": "{http://hl7.org/fhir}Patient",
                    },
                },
            ]
            + list(defs),
        },
    }
    if codesystems:
        lib["codeSystems"] = {"def": codesystems}
    if codes:
        lib["codes"] = {"def": codes}
    return lib


def _elm_library(*defs):
    """Wrap expression defs in a minimal ELM library structure."""
    return _elm_library_with_codes(
        {"id": "Test", "version": "1.0.0"}, None, None, *defs
    )


def _retrieve(resource_type="Patient", codes_expr=None, code_property=None):
    r = {"type": "Retrieve", "dataType": f"{{http://hl7.org/fhir}}{resource_type}"}
    if codes_expr:
        r["codes"] = codes_expr
    if code_property:
        r["codeProperty"] = code_property
    return r


def _code_ref(name):
    return {"type": "CodeRef", "name": name}


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
# Data seeding
# ---------------------------------------------------------------------------

def _create_dataset(client):
    did = _uid()
    status, body, _ = client.post("/datasets", {"id": did, "name": f"Measure {did}"})
    assert status == 201, f"create_dataset failed ({status}): {body}"
    return did


def _seed_test_data(client, dataset_id):
    """Create 3 patients (2 male, 1 female) with conditions and observations."""
    # Patient 1: male, has Condition C71.1 (brain cancer) and Observation 8310-5 (temperature)
    _, p1, _ = client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "gender": "male",
        "birthDate": "1985-03-15",
        "name": [{"family": "Mueller", "given": ["Hans"]}],
    })
    # Patient 2: male, has Condition J45.0 (asthma)
    _, p2, _ = client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "gender": "male",
        "birthDate": "1990-07-20",
        "name": [{"family": "Schmidt", "given": ["Karl"]}],
    })
    # Patient 3: female, has Condition C71.1 (brain cancer)
    _, p3, _ = client.post(f"/{dataset_id}/Patient", {
        "resourceType": "Patient",
        "gender": "female",
        "birthDate": "1978-11-02",
        "name": [{"family": "Fischer", "given": ["Anna"]}],
    })

    p1_id, p2_id, p3_id = p1["id"], p2["id"], p3["id"]

    # Conditions
    client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p1_id}"},
        "code": {"coding": [{"system": "http://fhir.de/CodeSystem/dimdi/icd-10-gm", "code": "C71.1"}]},
    })
    client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p2_id}"},
        "code": {"coding": [{"system": "http://fhir.de/CodeSystem/dimdi/icd-10-gm", "code": "J45.0"}]},
    })
    client.post(f"/{dataset_id}/Condition", {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{p3_id}"},
        "code": {"coding": [{"system": "http://fhir.de/CodeSystem/dimdi/icd-10-gm", "code": "C71.1"}]},
    })

    # Observations
    client.post(f"/{dataset_id}/Observation", {
        "resourceType": "Observation",
        "subject": {"reference": f"Patient/{p1_id}"},
        "code": {"coding": [{"system": "http://loinc.org", "code": "8310-5"}]},
        "status": "final",
    })

    return p1_id, p2_id, p3_id


def _post_library_and_measure(client, dataset_id, elm_library, measure_url="http://example.org/Measure/test", library_url="http://example.org/Library/test", expression_name="InInitialPopulation"):
    """POST a Library (with ELM content) and a Measure referencing it.

    Returns (measure_url, measure_id) — the canonical URL and server-assigned ID.
    """
    elm_b64 = _b64(json.dumps(elm_library))

    lib_resource = {
        "resourceType": "Library",
        "url": library_url,
        "version": "1.0.0",
        "status": "active",
        "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/library-type", "code": "logic-library"}]},
        "content": [
            {"contentType": "application/elm+json", "data": elm_b64},
        ],
    }
    s, body, _ = client.post(f"/{dataset_id}/Library", lib_resource)
    assert s == 201, f"POST Library failed ({s}): {body}"

    measure_resource = {
        "resourceType": "Measure",
        "url": measure_url,
        "version": "1.0.0",
        "status": "active",
        "subjectCodeableConcept": {
            "coding": [{"system": "http://hl7.org/fhir/resource-types", "code": "Patient"}],
        },
        "scoring": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/measure-scoring", "code": "cohort"}],
        },
        "library": [library_url],
        "group": [{
            "population": [{
                "code": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/measure-population", "code": "initial-population"}],
                },
                "criteria": {
                    "language": "text/cql-identifier",
                    "expression": expression_name,
                },
            }],
        }],
    }
    s, body, _ = client.post(f"/{dataset_id}/Measure", measure_resource)
    assert s == 201, f"POST Measure failed ({s}): {body}"

    return measure_url, body["id"]


# ---------------------------------------------------------------------------
# Module-scoped fixture: FHIR-only (no cql2elm needed for ELM-based tests)
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
# HAPPY PATH: Pre-compiled ELM
# ===================================================================

def test_measure_simple_condition(fhir):
    """exists [Condition: Code 'C71.1'] → count patients with brain cancer."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    assert body["resourceType"] == "MeasureReport"
    assert body["status"] == "complete"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 and p3 have C71.1


def test_measure_gender_filter(fhir):
    """Patient.gender = 'male' → count male patients."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [
                    _property("gender"),  # bare property → patient context
                    _literal("male"),
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 and p2 are male


def test_measure_and_condition_gender(fhir):
    """exists [Condition: Code 'C71.1'] and Patient.gender = 'male'."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "And",
                "operand": [
                    {
                        "type": "Exists",
                        "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
                    },
                    {
                        "type": "Equal",
                        "operand": [_property("gender"), _literal("male")],
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 1  # only p1: male AND has C71.1


def test_measure_or_multiple_conditions(fhir):
    """exists [Condition: Code 'C71.1'] or exists [Condition: Code 'J45.0']."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [
            {"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}},
            {"name": "J45.0 code", "id": "J45.0", "codeSystem": {"name": "icd10"}},
        ],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Or",
                "operand": [
                    {
                        "type": "Exists",
                        "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
                    },
                    {
                        "type": "Exists",
                        "operand": _retrieve("Condition", codes_expr=_code_ref("J45.0 code")),
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 3  # all three patients have at least one of these conditions


def test_measure_inclusion_exclusion(fhir):
    """Inclusion and not Exclusion — separate defines."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}}],
        {
            "name": "Inclusion",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
            },
        },
        {
            "name": "Exclusion",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [_property("gender"), _literal("female")],
            },
        },
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "And",
                "operand": [
                    {"type": "ExpressionRef", "name": "Inclusion"},
                    {
                        "type": "Not",
                        "operand": {"type": "ExpressionRef", "name": "Exclusion"},
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    # p1 has C71.1 and is male (not excluded) → included
    # p3 has C71.1 but is female (excluded)
    assert count == 1


def test_measure_observation_exists(fhir):
    """exists [Observation: Code '8310-5'] → count patients with temperature obs."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "loinc", "id": "http://loinc.org"}],
        [{"name": "temp code", "id": "8310-5", "codeSystem": {"name": "loinc"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Observation", codes_expr=_code_ref("temp code")),
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 1  # only p1 has the observation


def test_measure_no_matches(fhir):
    """Condition code not in dataset → count = 0."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "Z99.9 code", "id": "Z99.9", "codeSystem": {"name": "icd10"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("Z99.9 code")),
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 0


def test_measure_all_patients_match(fhir):
    """Patient.gender = 'male' or Patient.gender = 'female' → all patients."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Or",
                "operand": [
                    {
                        "type": "Equal",
                        "operand": [_property("gender"), _literal("male")],
                    },
                    {
                        "type": "Equal",
                        "operand": [_property("gender"), _literal("female")],
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(fhir, did, elm)
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 3


def test_measure_elm_content(fhir):
    """Library with pre-compiled ELM works without cql2elm extension."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [_property("gender"), _literal("female")],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/elm-test",
        library_url="http://example.org/Library/elm-test",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"evaluate-measure failed ({status}): {body}"
    assert body["resourceType"] == "MeasureReport"
    count = body["group"][0]["population"][0]["count"]
    assert count == 1  # only p3 is female


def test_measure_post_method(fhir):
    """$evaluate-measure also works via POST with Parameters body."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [_property("gender"), _literal("male")],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/post-test",
        library_url="http://example.org/Library/post-test",
    )
    status, body, _ = fhir.post(
        f"/{did}/Measure/$evaluate-measure",
        {
            "resourceType": "Parameters",
            "parameter": [
                {"name": "measure", "valueString": measure_url},
            ],
        },
    )
    assert status == 200, f"evaluate-measure POST failed ({status}): {body}"
    assert body["resourceType"] == "MeasureReport"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2


# ===================================================================
# ERROR CASES
# ===================================================================

def test_measure_missing_measure(fhir):
    """Invalid measure URL → 404."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure=http://example.org/nonexistent"
    )
    assert status == 404


def test_measure_missing_library(fhir):
    """Measure references nonexistent Library → 404."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    # Create a Measure that references a Library that doesn't exist
    measure_resource = {
        "resourceType": "Measure",
        "url": "http://example.org/Measure/orphan",
        "version": "1.0.0",
        "status": "active",
        "library": ["http://example.org/Library/nonexistent"],
        "group": [{
            "population": [{
                "code": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/measure-population", "code": "initial-population"}],
                },
                "criteria": {
                    "language": "text/cql-identifier",
                    "expression": "InInitialPopulation",
                },
            }],
        }],
    }
    s, _, _ = fhir.post(f"/{did}/Measure", measure_resource)
    assert s == 201

    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure=http://example.org/Measure/orphan"
    )
    assert status == 404


def test_measure_missing_measure_param(fhir):
    """No measure parameter → 400."""
    did = _create_dataset(fhir)
    status, body, _ = fhir.get(f"/{did}/Measure/$evaluate-measure")
    assert status == 400


# ===================================================================
# MII-STYLE TESTS: Instance-level routes and real MII patterns
# ===================================================================

def test_measure_instance_level(fhir):
    """MII actual call: GET /{dataset_id}/Measure/{measure_id}/$evaluate-measure."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
            },
        },
    )

    _, measure_id = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/inst-test",
        library_url="http://example.org/Library/inst-test",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/{measure_id}/$evaluate-measure?periodStart=2000-01-01&periodEnd=2030-12-31"
    )
    assert status == 200, f"instance evaluate-measure failed ({status}): {body}"
    assert body["resourceType"] == "MeasureReport"
    assert body["status"] == "complete"
    assert body["type"] == "summary"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 and p3 have C71.1


def test_measure_instance_level_post(fhir):
    """MII pattern: POST /{dataset_id}/Measure/{measure_id}/$evaluate-measure."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [_property("gender"), _literal("male")],
            },
        },
    )

    _, measure_id = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/inst-post-test",
        library_url="http://example.org/Library/inst-post-test",
    )
    # POST with no body (instance-level doesn't need measure param)
    status, body, _ = fhir.post(f"/{did}/Measure/{measure_id}/$evaluate-measure", {})
    assert status == 200, f"instance POST evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 and p2 are male


def test_measure_instance_level_invalid_id(fhir):
    """Instance-level with nonexistent measure ID → 404."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    status, body, _ = fhir.get(
        f"/{did}/Measure/nonexistent-id/$evaluate-measure"
    )
    assert status == 404


def test_measure_urn_uuid_urls(fhir):
    """MII uses urn:uuid: canonical URLs for Library and Measure."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    lib_uuid = f"urn:uuid:{uuid.uuid4()}"
    measure_uuid = f"urn:uuid:{uuid.uuid4()}"

    elm = _elm_library_with_codes(
        {"id": "Retrieve", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [{"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}}],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
            },
        },
    )

    _, measure_id = _post_library_and_measure(
        fhir, did, elm,
        measure_url=measure_uuid,
        library_url=lib_uuid,
    )
    # Use instance-level (MII pattern) since urn:uuid: URLs are awkward in query params
    status, body, _ = fhir.get(
        f"/{did}/Measure/{measure_id}/$evaluate-measure"
    )
    assert status == 200, f"urn:uuid evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 and p3 have C71.1
    # Verify MeasureReport references the canonical urn:uuid: URL
    assert body["measure"] == measure_uuid


def test_measure_multi_code_or(fhir):
    """MII feasibility: 5+ condition codes ORed together."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    # C71.1 (brain cancer): p1, p3
    # J45.0 (asthma): p2
    # Z99.9, K50.0, E11.9: nobody
    codes = [
        {"name": "c1", "id": "C71.1", "codeSystem": {"name": "icd10"}},
        {"name": "c2", "id": "J45.0", "codeSystem": {"name": "icd10"}},
        {"name": "c3", "id": "Z99.9", "codeSystem": {"name": "icd10"}},
        {"name": "c4", "id": "K50.0", "codeSystem": {"name": "icd10"}},
        {"name": "c5", "id": "E11.9", "codeSystem": {"name": "icd10"}},
    ]

    # Build a big OR of exists [Condition: Code 'X'] for each code
    or_operands = [
        {"type": "Exists", "operand": _retrieve("Condition", codes_expr=_code_ref(c["name"]))}
        for c in codes
    ]
    # Nested Or: (((a OR b) OR c) OR d) OR e
    expr = or_operands[0]
    for op in or_operands[1:]:
        expr = {"type": "Or", "operand": [expr, op]}

    elm = _elm_library_with_codes(
        {"id": "MultiCode", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        codes,
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": expr,
        },
    )

    measure_url, _ = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/multi-code",
        library_url="http://example.org/Library/multi-code",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"multi-code evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 3  # all three patients match at least one code


def test_measure_condition_and_observation(fhir):
    """MII pattern: Multiple resource type existence checks ANDed.

    exists [Condition: icd10 'C71.1'] AND exists [Observation: loinc '8310-5']
    → only p1 has both.
    """
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "MultiResource", "version": "1.0.0"},
        [
            {"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"},
            {"name": "loinc", "id": "http://loinc.org"},
        ],
        [
            {"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}},
            {"name": "temp code", "id": "8310-5", "codeSystem": {"name": "loinc"}},
        ],
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "And",
                "operand": [
                    {
                        "type": "Exists",
                        "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
                    },
                    {
                        "type": "Exists",
                        "operand": _retrieve("Observation", codes_expr=_code_ref("temp code")),
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/cond-obs",
        library_url="http://example.org/Library/cond-obs",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"cond+obs evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 1  # only p1 has both C71.1 and 8310-5


def test_measure_report_period(fhir):
    """Verify MeasureReport includes the period from request parameters."""
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library(
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "Equal",
                "operand": [_property("gender"), _literal("male")],
            },
        },
    )

    _, measure_id = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/period-test",
        library_url="http://example.org/Library/period-test",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/{measure_id}/$evaluate-measure?periodStart=2024-01-01&periodEnd=2024-12-31"
    )
    assert status == 200, f"period test failed ({status}): {body}"
    assert body["period"]["start"] == "2024-01-01"
    assert body["period"]["end"] == "2024-12-31"
    assert body["measure"] == "http://example.org/Measure/period-test"


def test_measure_chained_expression_refs(fhir):
    """MII pattern: Inclusion/Exclusion → Combined, with chained ExpressionRefs.

    define Condition1: exists [Condition: Code 'C71.1']
    define Condition2: exists [Condition: Code 'J45.0']
    define HasAnyCondition: Condition1 or Condition2
    define InInitialPopulation: HasAnyCondition and Patient.gender = 'male'
    → p1 (has C71.1, male) and p2 (has J45.0, male) = 2
    """
    did = _create_dataset(fhir)
    _seed_test_data(fhir, did)

    elm = _elm_library_with_codes(
        {"id": "Chained", "version": "1.0.0"},
        [{"name": "icd10", "id": "http://fhir.de/CodeSystem/dimdi/icd-10-gm"}],
        [
            {"name": "C71.1 code", "id": "C71.1", "codeSystem": {"name": "icd10"}},
            {"name": "J45.0 code", "id": "J45.0", "codeSystem": {"name": "icd10"}},
        ],
        {
            "name": "Condition1",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("C71.1 code")),
            },
        },
        {
            "name": "Condition2",
            "context": "Patient",
            "expression": {
                "type": "Exists",
                "operand": _retrieve("Condition", codes_expr=_code_ref("J45.0 code")),
            },
        },
        {
            "name": "HasAnyCondition",
            "context": "Patient",
            "expression": {
                "type": "Or",
                "operand": [
                    {"type": "ExpressionRef", "name": "Condition1"},
                    {"type": "ExpressionRef", "name": "Condition2"},
                ],
            },
        },
        {
            "name": "InInitialPopulation",
            "context": "Patient",
            "expression": {
                "type": "And",
                "operand": [
                    {"type": "ExpressionRef", "name": "HasAnyCondition"},
                    {
                        "type": "Equal",
                        "operand": [_property("gender"), _literal("male")],
                    },
                ],
            },
        },
    )

    measure_url, _ = _post_library_and_measure(
        fhir, did, elm,
        measure_url="http://example.org/Measure/chained",
        library_url="http://example.org/Library/chained",
    )
    status, body, _ = fhir.get(
        f"/{did}/Measure/$evaluate-measure?measure={measure_url}"
    )
    assert status == 200, f"chained expression evaluate-measure failed ({status}): {body}"
    count = body["group"][0]["population"][0]["count"]
    assert count == 2  # p1 (C71.1, male) and p2 (J45.0, male)
