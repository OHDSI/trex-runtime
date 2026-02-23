"""Migration standalone tests.

Verifies that the migration extension can apply SQL migrations,
track schema history, detect checksum mismatches, and report status.
"""

import os
import pytest

from conftest import REPO_ROOT

MIGRATIONS_DIR = os.path.join(REPO_ROOT, "plugins", "migration", "test", "sql", "migrations")
MIGRATIONS_TAMPERED_DIR = os.path.join(REPO_ROOT, "plugins", "migration", "test", "sql", "migrations_tampered")
MIGRATIONS_BAD_SQL_DIR = os.path.join(REPO_ROOT, "plugins", "migration", "test", "sql", "migrations_bad_sql")


def _node(node_factory):
    return node_factory(load_migration=True, load_db=False)


def test_migrate_apply(node_factory):
    """trex_migration_run() applies pending migrations and returns (version, name, status)."""
    node = _node(node_factory)
    result = node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    assert len(result) == 2
    assert result[0] == (1, "create_users", "applied")
    assert result[1] == (2, "add_email", "applied")


def test_migrate_creates_table(node_factory):
    """After trex_migration_run, DDL has actually run (table exists and accepts inserts)."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    node.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
    result = node.execute("SELECT * FROM users")
    assert len(result) == 1
    assert result[0] == (1, "Alice", "alice@example.com")


def test_migrate_rerun_skipped(node_factory):
    """Re-running trex_migration_run on already-applied migrations returns all 'skipped'."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    result = node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    assert len(result) == 2
    assert result[0] == (1, "create_users", "skipped")
    assert result[1] == (2, "add_email", "skipped")


def test_schema_history(node_factory):
    """refinery_schema_history contains version and name for applied migrations."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    result = node.execute(
        "SELECT version, name FROM refinery_schema_history ORDER BY version"
    )
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[0][1] == "create_users"
    assert result[1][0] == 2
    assert result[1][1] == "add_email"


def test_migration_status_applied(node_factory):
    """trex_migration_status() shows 'applied' after migrate."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    result = node.execute(
        f"SELECT version, name, status FROM trex_migration_status('{MIGRATIONS_DIR}')"
    )
    assert len(result) == 2
    assert result[0] == (1, "create_users", "applied")
    assert result[1] == (2, "add_email", "applied")


def test_migrate_checksum_mismatch(node_factory):
    """trex_migration_run() raises error when a previously-applied migration has been tampered."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    with pytest.raises(RuntimeError, match="Checksum mismatch"):
        node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_TAMPERED_DIR}')")


def test_migration_status_checksum_mismatch(node_factory):
    """trex_migration_status() reports checksum_mismatch for tampered migrations."""
    node = _node(node_factory)
    node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_DIR}')")
    result = node.execute(
        f"SELECT version, name, status FROM trex_migration_status('{MIGRATIONS_TAMPERED_DIR}')"
    )
    assert result[0] == (1, "create_users", "checksum_mismatch")


def test_migrate_invalid_sql(node_factory):
    """trex_migration_run() raises error for migration files with invalid SQL."""
    node = _node(node_factory)
    with pytest.raises(RuntimeError, match="Migration V99__bad_migration failed"):
        node.execute(f"SELECT * FROM trex_migration_run('{MIGRATIONS_BAD_SQL_DIR}')")


def test_migrate_directory_not_found(node_factory):
    """trex_migration_run() raises error for non-existent directory."""
    node = _node(node_factory)
    with pytest.raises(RuntimeError, match="Directory not found"):
        node.execute("SELECT * FROM trex_migration_run('/nonexistent/path/to/migrations')")


def test_migration_status_directory_not_found(node_factory):
    """trex_migration_status() raises error for non-existent directory."""
    node = _node(node_factory)
    with pytest.raises(RuntimeError, match="Directory not found"):
        node.execute("SELECT * FROM trex_migration_status('/nonexistent/path/to/migrations')")


def test_migrate_ordering(node_factory, tmp_path):
    """Migrations are applied in version order regardless of file creation order."""
    # Create files out of order: V3, V1, V2
    (tmp_path / "V3__third.sql").write_text("CREATE TABLE t3(x INTEGER);")
    (tmp_path / "V1__first.sql").write_text("CREATE TABLE t1(x INTEGER);")
    (tmp_path / "V2__second.sql").write_text("CREATE TABLE t2(x INTEGER);")

    node = _node(node_factory)
    result = node.execute(f"SELECT * FROM trex_migration_run('{tmp_path}')")
    assert len(result) == 3
    assert result[0] == (1, "first", "applied")
    assert result[1] == (2, "second", "applied")
    assert result[2] == (3, "third", "applied")
