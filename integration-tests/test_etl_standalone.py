"""ETL standalone tests.

Verifies that the etl extension loads and its SQL functions respond correctly.
Full CDC pipeline tests require a PostgreSQL instance with logical replication.
"""

import pytest


def test_etl_extension_loads(node_factory):
    """Extension loads without errors."""
    node = node_factory(load_etl=True, load_db=False)
    result = node.execute("SELECT 1")
    assert result == [(1,)]


def test_etl_status_empty(node_factory):
    """trex_etl_status() returns empty result when no pipelines are running."""
    node = node_factory(load_etl=True, load_db=False)
    result = node.execute("SELECT * FROM trex_etl_status()")
    assert result == []


def test_etl_stop_nonexistent(node_factory):
    """trex_etl_stop() returns error for a pipeline that does not exist."""
    node = node_factory(load_etl=True, load_db=False)
    with pytest.raises(RuntimeError, match="not found"):
        node.execute("SELECT trex_etl_stop('nonexistent')")


def test_etl_start_missing_publication(node_factory):
    """trex_etl_start() returns error when connection string lacks publication."""
    node = node_factory(load_etl=True, load_db=False)
    with pytest.raises(RuntimeError, match="publication"):
        node.execute(
            "SELECT trex_etl_start('test_pipe', "
            "'host=localhost port=5432 dbname=test user=test password=test')"
        )
