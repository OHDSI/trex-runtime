"""Atlas standalone tests.

Verifies that the atlas extension can load and execute OHDSI Circe functions:
SQL rendering, translation, cohort JSON-to-SQL, and combined operations.
"""

import pytest


# Minimal OHDSI cohort expression (base64-encoded JSON)
COHORT_B64 = (
    "ewogICJQcmltYXJ5Q3JpdGVyaWEiOiB7CiAgICAiQ3JpdGVyaWFMaXN0IjogW10s"
    "CiAgICAiT2JzZXJ2YXRpb25XaW5kb3ciOiB7IlByaW9yRGF5cyI6IDAsICJQb3N0"
    "RGF5cyI6IDB9LAogICAgIlByaW1hcnlDcml0ZXJpYUxpbWl0IjogeyJUeXBlIjog"
    "IkFsbCJ9CiAgfSwKICAiQ29uY2VwdFNldHMiOiBbXSwKICAiUXVhbGlmaWVkTGlt"
    "aXQiOiB7IlR5cGUiOiAiRmlyc3QifSwKICAiRXhwcmVzc2lvbkxpbWl0IjogeyJU"
    "eXBlIjogIkFsbCJ9LAogICJJbmNsdXNpb25SdWxlcyI6IFtdCn0="
)

OPTIONS_JSON = (
    '{"cdmSchema":"cdm","resultSchema":"results",'
    '"targetTable":"cohort","cohortId":42,"generateStats":false}'
)


def test_atlas_load_and_hello(node_factory):
    """Extension loads and trex_atlas_hello() returns greeting."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute("SELECT trex_atlas_hello('Sam')")
    assert len(result) == 1
    assert result[0][0] == "Circe Sam"


def test_atlas_openssl_version(node_factory):
    """trex_atlas_openssl_version() reports linked OpenSSL version."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute("SELECT trex_atlas_openssl_version('Test')")
    assert len(result) == 1
    assert "OpenSSL" in result[0][0]


def test_atlas_sql_render(node_factory):
    """trex_atlas_sql_render() substitutes @-parameters into SQL template."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute(
        "SELECT trex_atlas_sql_render("
        "'SELECT * FROM @schema.patients WHERE age = @age;', "
        "'{\"schema\": \"cdm\", \"age\": \"25\"}')"
    )
    assert len(result) == 1
    sql = result[0][0]
    assert "cdm.patients" in sql
    assert "25" in sql


def test_atlas_sql_translate(node_factory):
    """trex_atlas_sql_translate() converts SQL Server syntax to target dialect."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute(
        "SELECT trex_atlas_sql_translate("
        "'SELECT TOP 10 * FROM patients;', 'postgresql')"
    )
    assert len(result) == 1
    sql = result[0][0]
    assert "LIMIT 10" in sql


def test_atlas_sql_render_translate(node_factory):
    """trex_atlas_sql_render_translate() renders parameters then translates dialect."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute(
        "SELECT trex_atlas_sql_render_translate("
        "'SELECT TOP @limit * FROM @schema.patients;', "
        "'postgresql', "
        "'{\"limit\": \"5\", \"schema\": \"cdm\"}')"
    )
    assert len(result) == 1
    sql = result[0][0]
    assert "LIMIT 5" in sql
    assert "cdm.patients" in sql


def test_atlas_json_to_sql(node_factory):
    """trex_atlas_json_to_sql() converts base64 OHDSI cohort JSON to SQL."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute(
        f"SELECT trex_atlas_json_to_sql('{COHORT_B64}', '{OPTIONS_JSON}')"
    )
    assert len(result) == 1
    sql = result[0][0].lower()
    assert "select" in sql
    assert "cohort" in sql


def test_atlas_generate_and_translate(node_factory):
    """trex_atlas_generate_and_translate() produces trexsql-dialect SQL from cohort JSON."""
    node = node_factory(load_atlas=True, load_db=False)
    result = node.execute(
        f"SELECT trex_atlas_generate_and_translate('{COHORT_B64}', '{OPTIONS_JSON}')"
    )
    assert len(result) == 1
    sql = result[0][0]
    assert "SELECT" in sql or "select" in sql.lower()
