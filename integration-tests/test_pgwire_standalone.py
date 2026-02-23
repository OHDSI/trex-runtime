"""PgWire standalone tests.

Verifies that the pgwire extension can load, start/stop the wire-protocol
server, and serve queries to standard PostgreSQL clients (psycopg2).
"""

import psycopg2
import pytest
from conftest import wait_for


def test_pgwire_load_and_version(node_factory):
    """Extension loads and trex_pgwire_version() returns a version string."""
    node = node_factory(load_pgwire=True, load_db=False)
    result = node.execute("SELECT trex_pgwire_version()")
    assert len(result) == 1
    assert "pgwire" in result[0][0].lower()


def test_pgwire_server_lifecycle(node_factory):
    """Start server, verify status shows running, stop, verify status empty."""
    node = node_factory(load_pgwire=True, load_db=False)

    # Start pgwire server (empty password, empty credentials)
    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, '', '')"
    )

    # Verify server is running
    status = node.execute("SELECT * FROM trex_pgwire_status()")
    assert len(status) == 1, f"Expected 1 status row, got {len(status)}"
    assert status[0][0] == "127.0.0.1"  # hostname
    assert status[0][1] == str(node.pgwire_port)  # port

    # Stop pgwire server
    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_pgwire_psycopg2_select(node_factory):
    """Start server, connect with psycopg2, run SELECT 42."""
    node = node_factory(load_pgwire=True, load_db=False)

    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT 42")
        rows = cur.fetchall()
        assert rows == [(42,)]
        cur.close()
    finally:
        conn.close()

    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_pgwire_psycopg2_create_and_query(node_factory):
    """DDL/DML via psycopg2: CREATE TABLE, INSERT, SELECT, verify rows."""
    node = node_factory(load_pgwire=True, load_db=False)

    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        cur.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
        cur.execute("SELECT id, name FROM items ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]
        cur.close()
    finally:
        conn.close()

    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_pgwire_data_visibility(node_factory):
    """Table created via trexsql node is visible through pgwire."""
    node = node_factory(load_pgwire=True, load_db=False)

    # Create table through the trexsql node directly
    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region FROM range(10) t(i)"
    )

    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        rows = cur.fetchall()
        assert rows == [(10,)]
        cur.close()
    finally:
        conn.close()

    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_pgwire_scram_auth_with_password(node_factory):
    """Start server with password, wrong password fails, correct succeeds."""
    node = node_factory(load_pgwire=True, load_db=False)

    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'secret', '')"
    )

    # Wrong password should fail
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(
            host="127.0.0.1",
            port=node.pgwire_port,
            user="any",
            password="wrong",
            dbname="memory",
        )

    # Correct password should succeed
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="secret",
        dbname="memory",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        rows = cur.fetchall()
        assert rows == [(1,)]
        cur.close()
    finally:
        conn.close()

    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_pgwire_server_status_after_stop(node_factory):
    """After stop, trex_pgwire_status() returns 0 rows."""
    node = node_factory(load_pgwire=True, load_db=False)

    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, '', '')"
    )

    # Confirm it's running
    status = node.execute("SELECT * FROM trex_pgwire_status()")
    assert len(status) == 1

    # Stop
    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )

    # Status should be empty after stop
    status = wait_for(
        node,
        "SELECT * FROM trex_pgwire_status()",
        lambda rows: len(rows) == 0,
        timeout=5,
    )
    assert len(status) == 0
