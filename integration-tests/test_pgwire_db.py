"""PgWire + DB integration tests.

Verifies that pgwire can be registered and discovered as a db service,
and that multiple pgwire nodes work with gossip-based service discovery.
"""

import psycopg2
from conftest import wait_for


def test_db_register_pgwire(node_factory):
    """Register pgwire as a db service and verify it appears in trex_db_services()."""
    node = node_factory(load_pgwire=True, load_db=True)

    # Start pgwire server
    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )

    # Start db
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register pgwire service
    node.execute(
        f"SELECT trex_db_register_service('pgwire', '127.0.0.1', {node.pgwire_port})"
    )

    # Verify pgwire shows up in trex_db_services()
    services = wait_for(
        node,
        "SELECT * FROM trex_db_services()",
        lambda rows: any(r[1] == "pgwire" for r in rows),
        timeout=10,
    )
    pgwire_rows = [r for r in services if r[1] == "pgwire"]
    assert len(pgwire_rows) >= 1
    assert pgwire_rows[0][4] == "running"  # status column


def test_db_start_pgwire_service(node_factory):
    """trex_db_start_service('pgwire', json_config) starts server and registers in gossip."""
    node = node_factory(load_pgwire=True, load_db=True)

    # Start db first
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Use trex_db_start_service to start pgwire with JSON config
    node.execute(
        f"""SELECT trex_db_start_service('pgwire', '{{"host": "127.0.0.1", "port": {node.pgwire_port}, "password": "test"}}')"""
    )

    # Verify pgwire is in trex_db_services
    services = wait_for(
        node,
        "SELECT * FROM trex_db_services()",
        lambda rows: any(r[1] == "pgwire" for r in rows),
        timeout=10,
    )
    pgwire_rows = [r for r in services if r[1] == "pgwire"]
    assert len(pgwire_rows) >= 1

    # Verify psycopg2 can connect
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=node.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
        cur.close()
    finally:
        conn.close()


def test_two_node_pgwire_discovery(node_factory):
    """Two nodes register pgwire; both visible in trex_db_services() from either node."""
    node_a = node_factory(load_pgwire=True, load_db=True)
    node_b = node_factory(load_pgwire=True, load_db=True)

    # Node A: start db + pgwire
    node_a.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node_a.pgwire_port}, 'test', '')"
    )
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute(
        f"SELECT trex_db_register_service('pgwire', '127.0.0.1', {node_a.pgwire_port})"
    )

    # Node B: start db + pgwire, join Node A
    node_b.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node_b.pgwire_port}, 'test', '')"
    )
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{node_a.gossip_port}')"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('pgwire', '127.0.0.1', {node_b.pgwire_port})"
    )

    # Wait for gossip convergence -- both nodes see 2 pgwire services
    wait_for(
        node_a,
        "SELECT * FROM trex_db_services()",
        lambda rows: len([r for r in rows if r[1] == "pgwire"]) >= 2,
        timeout=15,
    )
    services_b = wait_for(
        node_b,
        "SELECT * FROM trex_db_services()",
        lambda rows: len([r for r in rows if r[1] == "pgwire"]) >= 2,
        timeout=15,
    )
    pgwire_rows = [r for r in services_b if r[1] == "pgwire"]
    assert len(pgwire_rows) >= 2


def test_two_node_pgwire_data_isolation(node_factory):
    """Two nodes each have different tables; psycopg2 to each sees only local data."""
    node_a = node_factory(load_pgwire=True, load_db=True)
    node_b = node_factory(load_pgwire=True, load_db=True)

    # Node A: US data
    node_a.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region FROM range(100) t(i)"
    )
    node_a.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node_a.pgwire_port}, 'test', '')"
    )

    # Node B: EU data
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'EU' as region FROM range(200) t(i)"
    )
    node_b.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node_b.pgwire_port}, 'test', '')"
    )

    # Query Node A via psycopg2
    conn_a = psycopg2.connect(
        host="127.0.0.1",
        port=node_a.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        cur = conn_a.cursor()
        cur.execute("SELECT region, COUNT(*) FROM orders GROUP BY region")
        rows_a = cur.fetchall()
        cur.close()
    finally:
        conn_a.close()

    # Query Node B via psycopg2
    conn_b = psycopg2.connect(
        host="127.0.0.1",
        port=node_b.pgwire_port,
        user="any",
        password="test",
        dbname="memory",
    )
    try:
        cur = conn_b.cursor()
        cur.execute("SELECT region, COUNT(*) FROM orders GROUP BY region")
        rows_b = cur.fetchall()
        cur.close()
    finally:
        conn_b.close()

    # Node A should only have US data
    assert len(rows_a) == 1
    assert rows_a[0][0] == "US"
    assert rows_a[0][1] == 100

    # Node B should only have EU data
    assert len(rows_b) == 1
    assert rows_b[0][0] == "EU"
    assert rows_b[0][1] == 200


def test_pgwire_flight_coexistence(node_factory):
    """Single node runs both flight + pgwire simultaneously; both work."""
    node = node_factory(load_pgwire=True, load_db=True)

    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region FROM range(50) t(i)"
    )

    # Start both servers
    node.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node.flight_port})")
    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )

    # Verify flight is running
    flight_status = node.execute("SELECT * FROM trex_db_flight_status()")
    assert len(flight_status) > 0, "Flight server should be running"

    # Verify pgwire is running
    pgwire_status = node.execute("SELECT * FROM trex_pgwire_status()")
    assert len(pgwire_status) > 0, "PgWire server should be running"

    # Query via psycopg2 (pgwire)
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
        assert rows == [(50,)]
        cur.close()
    finally:
        conn.close()

    # Stop both
    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )
    node.execute(
        f"SELECT trex_db_flight_stop('0.0.0.0', {node.flight_port})"
    )
