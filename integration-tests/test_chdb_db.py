"""ChDB + DB integration tests.

Verifies that chdb can be registered as a metadata-only db service
(port=0, no real TCP listener) and discovered via gossip across nodes.
"""

from conftest import wait_for


def test_db_register_chdb(node_factory):
    """Start chdb + db, register chdb as service, verify it appears in trex_db_services()."""
    node = node_factory(load_chdb=True, load_db=True)

    # Start chdb engine
    node.execute("SELECT trex_chdb_start('')")

    # Start db
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register chdb as metadata-only service (port=0, no real listener)
    node.execute("SELECT trex_db_register_service('chdb', '127.0.0.1', 0)")

    # Verify chdb shows up in trex_db_services()
    services = wait_for(
        node,
        "SELECT * FROM trex_db_services()",
        lambda rows: any(r[1] == "chdb" for r in rows),
        timeout=10,
    )
    chdb_rows = [r for r in services if r[1] == "chdb"]
    assert len(chdb_rows) >= 1
    assert chdb_rows[0][4] == "running"  # status column


def test_chdb_queries_with_db(node_factory):
    """chdb queries still work while db is active."""
    node = node_factory(load_chdb=True, load_db=True)

    # Start chdb + db + register
    node.execute("SELECT trex_chdb_start('')")
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )
    node.execute("SELECT trex_db_register_service('chdb', '127.0.0.1', 0)")

    # Run a chdb query -- should work fine alongside db
    result = node.execute("SELECT * FROM trex_chdb_scan('SELECT 1 as a')")
    assert len(result) == 1
    assert result[0][0] == "1"


def test_two_node_chdb_discovery(node_factory):
    """Two nodes both register chdb; both visible in trex_db_services() via gossip."""
    node_a = node_factory(load_chdb=True, load_db=True)
    node_b = node_factory(load_chdb=True, load_db=True)

    # Node A: start chdb + db, register chdb
    node_a.execute("SELECT trex_chdb_start('')")
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute("SELECT trex_db_register_service('chdb', '127.0.0.1', 0)")

    # Node B: start chdb + db (join Node A via seeds), register chdb
    node_b.execute("SELECT trex_chdb_start('')")
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{node_a.gossip_port}')"
    )
    node_b.execute("SELECT trex_db_register_service('chdb', '127.0.0.1', 0)")

    # Wait for gossip convergence -- both nodes see 2 chdb services
    wait_for(
        node_a,
        "SELECT * FROM trex_db_services()",
        lambda rows: len([r for r in rows if r[1] == "chdb"]) >= 2,
        timeout=15,
    )
    services_b = wait_for(
        node_b,
        "SELECT * FROM trex_db_services()",
        lambda rows: len([r for r in rows if r[1] == "chdb"]) >= 2,
        timeout=15,
    )
    chdb_rows = [r for r in services_b if r[1] == "chdb"]
    assert len(chdb_rows) >= 2


def test_chdb_service_coexistence(node_factory):
    """Single node runs flight + db + chdb; both services registered and visible."""
    node = node_factory(load_chdb=True, load_db=True)

    # Start all services
    node.execute("SELECT trex_chdb_start('')")
    node.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node.flight_port})")
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )

    # Register both services
    node.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node.flight_port})"
    )
    node.execute("SELECT trex_db_register_service('chdb', '127.0.0.1', 0)")

    # Verify both service types appear in trex_db_services()
    services = wait_for(
        node,
        "SELECT * FROM trex_db_services()",
        lambda rows: (
            any(r[1] == "chdb" for r in rows)
            and any(r[1] == "flight" for r in rows)
        ),
        timeout=10,
    )
    chdb_rows = [r for r in services if r[1] == "chdb"]
    flight_rows = [r for r in services if r[1] == "flight"]
    assert len(chdb_rows) >= 1
    assert len(flight_rows) >= 1
