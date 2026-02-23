"""pg_trex integration tests.

Connects to a pg_trex PostgreSQL container (port 45432) and verifies that the
embedded trexsql engine works correctly through the PostgreSQL interface.
"""

import time

import psycopg2
import pytest


# Per-test timeout (seconds) to prevent indefinite hangs in CI.
QUERY_TIMEOUT = 60


def _pg_trex_reachable():
    """Check if the pg_trex container is reachable."""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", port=45432, user="postgres", dbname="postgres"
        )
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_trex_reachable(),
    reason="pg_trex container not running on port 45432",
)


@pytest.fixture(scope="module")
def pg_conn():
    """Module-scoped psycopg2 connection to the pg_trex container."""
    conn = psycopg2.connect(
        host="127.0.0.1", port=45432, user="postgres", dbname="postgres",
        options=f"-c statement_timeout={QUERY_TIMEOUT * 1000}",
    )
    conn.autocommit = True
    yield conn
    conn.close()


def _wait_for_worker(conn, timeout=30):
    """Poll pg_trex_status() until state=running or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur = conn.cursor()
        cur.execute("SELECT state FROM pg_trex_status()")
        row = cur.fetchone()
        cur.close()
        if row and row[0] == "running":
            return
        time.sleep(1)
    raise TimeoutError(f"pg_trex worker did not reach 'running' within {timeout}s")


def test_pg_trex_extension_installed(pg_conn):
    """Verify pg_trex is listed in pg_extension."""
    cur = pg_conn.cursor()
    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_trex'")
    row = cur.fetchone()
    cur.close()
    assert row is not None, "pg_trex extension not found in pg_extension"
    assert row[0] == "pg_trex"


def test_pg_trex_status(pg_conn):
    """Verify pg_trex_status() returns state=running."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    cur.execute("SELECT state FROM pg_trex_status()")
    row = cur.fetchone()
    cur.close()
    assert row is not None
    assert row[0] == "running"


def test_pg_trex_query_select_literal(pg_conn):
    """pg_trex_query('SELECT 42') returns '42'."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    cur.execute("SELECT result FROM pg_trex_query('SELECT 42 AS val')")
    row = cur.fetchone()
    cur.close()
    assert row is not None
    assert "42" in row[0]


def test_pg_trex_query_create_and_select(pg_conn):
    """DDL + query via pg_trex_query."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    cur.execute("SELECT result FROM pg_trex_query('CREATE TABLE test_t (id INTEGER, name VARCHAR)')")
    cur.fetchall()
    cur.execute("SELECT result FROM pg_trex_query('INSERT INTO test_t VALUES (1, ''hello''), (2, ''world'')')")
    cur.fetchall()
    cur.execute("SELECT result FROM pg_trex_query('SELECT id, name FROM test_t ORDER BY id')")
    rows = cur.fetchall()
    cur.close()
    assert len(rows) == 2
    assert "1" in rows[0][0]
    assert "hello" in rows[0][0]
    assert "2" in rows[1][0]
    assert "world" in rows[1][0]


def test_pg_trex_query_multiple_columns(pg_conn):
    """Multi-column results are tab-separated."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    cur.execute("SELECT result FROM pg_trex_query('SELECT 1 AS a, 2 AS b, 3 AS c')")
    row = cur.fetchone()
    cur.close()
    assert row is not None
    parts = row[0].split("\t")
    assert len(parts) == 3
    assert parts[0].strip() == "1"
    assert parts[1].strip() == "2"
    assert parts[2].strip() == "3"


def test_pg_trex_query_error_handling(pg_conn):
    """Invalid SQL raises a psycopg2 error."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT result FROM pg_trex_query('SELEC INVALID SYNTAX')")
        cur.fetchall()
    cur.close()


def test_pg_trex_distributed_query_select(pg_conn):
    """pg_trex_distributed_query works on a single-node cluster."""
    _wait_for_worker(pg_conn)
    cur = pg_conn.cursor()
    cur.execute("SELECT result FROM pg_trex_distributed_query('SELECT 1 AS val')")
    row = cur.fetchone()
    cur.close()
    assert row is not None
    assert "1" in row[0]
