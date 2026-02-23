"""CQL endpoint performance benchmarks.

Seeds datasets of varying sizes and measures CQL query latency.
Run directly:  python perf_fhir_cql.py
"""

import json
import os
import socket
import statistics
import sys
import time
import urllib.request
import urllib.error
import uuid

# ---------------------------------------------------------------------------
# Must run from integration-tests/ so conftest is importable
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from conftest import Node, FHIR_EXT, alloc_ports

os.environ.setdefault("FHIR_POOL_SIZE", "1")

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class FhirClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, path, data=None):
        url = f"{self.base_url}{path}"
        body_bytes = json.dumps(data).encode("utf-8") if data is not None else None
        req = urllib.request.Request(url, data=body_bytes, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            return e.code, json.loads(raw) if raw else None

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, data):
        return self.request("POST", path, data)


# ---------------------------------------------------------------------------
# ELM helpers
# ---------------------------------------------------------------------------

def _elm_library(*defs):
    all_defs = [
        {"name": "Patient", "context": "Patient",
         "expression": {"type": "Retrieve", "dataType": "{http://hl7.org/fhir}Patient"}},
    ] + list(defs)
    return {"identifier": {"id": "Perf", "version": "1.0.0"}, "statements": {"def": all_defs}}

def _retrieve(rt="Patient"):
    return {"type": "Retrieve", "dataType": f"{{http://hl7.org/fhir}}{rt}"}

def _property(path, scope=None):
    p = {"type": "Property", "path": path}
    if scope:
        p["scope"] = scope
    return p

def _literal(value, vtype="String"):
    return {"type": "Literal", "valueType": f"{{urn:hl7-org:elm-types:r1}}{vtype}", "value": str(value)}


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

QUERIES = {
    "simple_retrieve": lambda: _elm_library(
        {"name": "All", "context": "Patient", "expression": _retrieve()}
    ),
    "filter_gender": lambda: _elm_library(
        {"name": "Males", "context": "Patient", "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve()}],
            "where": {"type": "Equal", "operand": [
                _property("gender", "P"), _literal("male")]},
        }}
    ),
    "filter_birthdate": lambda: _elm_library(
        {"name": "Recent", "context": "Patient", "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve()}],
            "where": {"type": "GreaterOrEqual", "operand": [
                _property("birthDate", "P"), _literal("2000-01-01", "Date")]},
        }}
    ),
    "count": lambda: _elm_library(
        {"name": "Cnt", "context": "Patient", "expression": {
            "type": "Count", "source": _retrieve()}}
    ),
    "property_extract": lambda: _elm_library(
        {"name": "Dates", "context": "Patient", "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve()}],
            "return": {"expression": _property("birthDate", "P")},
        }}
    ),
    "chained_count": lambda: _elm_library(
        {"name": "Males", "context": "Patient", "expression": {
            "type": "Query",
            "source": [{"alias": "P", "expression": _retrieve()}],
            "where": {"type": "Equal", "operand": [
                _property("gender", "P"), _literal("male")]},
        }},
        {"name": "MaleCnt", "context": "Patient", "expression": {
            "type": "Count",
            "source": {"type": "ExpressionRef", "name": "Males"},
        }},
    ),
}


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
               "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
               "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez"]
GENDERS = ["male", "female"]


def seed_patients(client, dataset_id, count):
    """Seed patients using transaction bundles for speed."""
    batch_size = 50
    created = 0
    while created < count:
        n = min(batch_size, count - created)
        entries = []
        for i in range(n):
            idx = created + i
            year = 1950 + (idx % 70)
            month = 1 + (idx % 12)
            day = 1 + (idx % 28)
            entries.append({
                "request": {"method": "POST", "url": "Patient"},
                "resource": {
                    "resourceType": "Patient",
                    "name": [{"family": LAST_NAMES[idx % len(LAST_NAMES)],
                              "given": [FIRST_NAMES[idx % len(FIRST_NAMES)]]}],
                    "gender": GENDERS[idx % 2],
                    "birthDate": f"{year:04d}-{month:02d}-{day:02d}",
                },
            })
        bundle = {"resourceType": "Bundle", "type": "transaction", "entry": entries}
        status, body = client.post(f"/{dataset_id}", bundle)
        if status != 200:
            print(f"  [WARN] seed batch failed ({status}): {body}")
            return created
        created += n
    return created


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_benchmark(client, dataset_id, query_name, elm_factory, iterations=20, warmup=3):
    """Run a CQL query multiple times, return timing stats in ms."""
    elm = elm_factory()
    payload = {"library": elm}
    path = f"/{dataset_id}/$cql"

    # Warmup
    for _ in range(warmup):
        client.post(path, payload)

    times = []
    errors = 0
    for _ in range(iterations):
        t0 = time.perf_counter()
        status, body = client.post(path, payload)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if status == 200:
            times.append(elapsed_ms)
        else:
            errors += 1

    if not times:
        return {"query": query_name, "error": f"all {errors} iterations failed"}

    return {
        "query": query_name,
        "iterations": len(times),
        "errors": errors,
        "min_ms": round(min(times), 2),
        "median_ms": round(statistics.median(times), 2),
        "mean_ms": round(statistics.mean(times), 2),
        "p95_ms": round(sorted(times)[int(len(times) * 0.95)], 2),
        "max_ms": round(max(times), 2),
    }


def main():
    print("=" * 70)
    print("CQL Performance Benchmark")
    print("=" * 70)

    # Start server
    gp, fp, pp = alloc_ports()
    node = Node([FHIR_EXT], gp, fp, pp)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        fhir_port = s.getsockname()[1]

    result = node.execute(f"SELECT fhir_start('127.0.0.1', {fhir_port})")
    assert "Started" in result[0][0], f"fhir_start: {result}"
    client = FhirClient(f"http://127.0.0.1:{fhir_port}")

    # Wait for health
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            s, _ = client.get("/health")
            if s == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    else:
        node.close()
        sys.exit("Server did not become healthy")

    print(f"\nServer running on port {fhir_port}\n")

    dataset_sizes = [10, 100, 1000]
    iterations = 30

    for size in dataset_sizes:
        did = f"perf-{uuid.uuid4().hex[:6]}"
        status, _ = client.post("/datasets", {"id": did, "name": f"Perf {size}"})
        assert status == 201, f"create dataset failed: {status}"

        print(f"--- {size} patients ---")
        t0 = time.perf_counter()
        actual = seed_patients(client, did, size)
        seed_ms = (time.perf_counter() - t0) * 1000
        print(f"  Seeded {actual} patients in {seed_ms:.0f} ms "
              f"({seed_ms / max(actual, 1):.1f} ms/patient)\n")

        print(f"  {'Query':<22} {'Min':>8} {'Median':>8} {'Mean':>8} {'P95':>8} {'Max':>8}  (ms, n={iterations})")
        print(f"  {'-'*22} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

        for name, factory in QUERIES.items():
            result = run_benchmark(client, did, name, factory, iterations=iterations)
            if "error" in result:
                print(f"  {name:<22} ERROR: {result['error']}")
            else:
                print(f"  {name:<22} {result['min_ms']:>8.2f} {result['median_ms']:>8.2f} "
                      f"{result['mean_ms']:>8.2f} {result['p95_ms']:>8.2f} {result['max_ms']:>8.2f}"
                      f"{'  (' + str(result['errors']) + ' err)' if result['errors'] else ''}")
        print()

    # Cleanup
    try:
        node.execute(f"SELECT fhir_stop('127.0.0.1', {fhir_port})")
    except Exception:
        pass
    node.close()
    print("Done.")


if __name__ == "__main__":
    main()
