"""Standalone tests for the cql2elm DuckDB extension.

Tests the trex_fhir_cql_translate() scalar function directly via SQL.
Requires the cql2elm extension to be built first.
"""

import json

import pytest

from conftest import CQL2ELM_EXT, Node, alloc_ports


def _sql_escape(s):
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


@pytest.fixture(scope="module")
def cql2elm_node():
    gp, fp, pp = alloc_ports()
    node = Node([CQL2ELM_EXT], gp, fp, pp)
    yield node
    node.close()


def test_simple_translation(cql2elm_node):
    """Minimal CQL library translates to valid ELM JSON with identifier."""
    cql = (
        "library Test version '1.0.0' "
        "using FHIR version '4.0.1' "
        "context Patient "
        "define AllPatients: [Patient]"
    )
    escaped = _sql_escape(cql)
    result = cql2elm_node.execute(f"SELECT trex_fhir_cql_translate('{escaped}')")
    assert len(result) == 1
    elm_json = result[0][0]
    elm = json.loads(elm_json)
    # ELM JSON has a "library" wrapper
    lib = elm.get("library", elm)
    assert "identifier" in lib
    assert lib["identifier"]["id"] == "Test"
    assert lib["identifier"]["version"] == "1.0.0"


def test_cql_with_filter(cql2elm_node):
    """CQL with a where clause translates to ELM containing a Query node."""
    cql = (
        "library FilterTest version '1.0.0' "
        "using FHIR version '4.0.1' "
        "context Patient "
        "define MalePatients: [Patient] P where P.gender = 'male'"
    )
    escaped = _sql_escape(cql)
    result = cql2elm_node.execute(f"SELECT trex_fhir_cql_translate('{escaped}')")
    assert len(result) == 1
    elm = json.loads(result[0][0])
    lib = elm.get("library", elm)
    assert lib["identifier"]["id"] == "FilterTest"
    # Should have statements
    assert "statements" in lib


def test_null_input(cql2elm_node):
    """NULL input returns NULL."""
    result = cql2elm_node.execute("SELECT trex_fhir_cql_translate(NULL)")
    assert len(result) == 1
    assert result[0][0] is None


def test_invalid_cql(cql2elm_node):
    """Invalid CQL syntax raises an error."""
    with pytest.raises(RuntimeError, match="(?i)(error|translation)"):
        cql2elm_node.execute("SELECT trex_fhir_cql_translate('this is not valid CQL at all')")
