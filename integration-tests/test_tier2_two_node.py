"""Tier 2: Two-node cluster tests.

Based on quickstart.md "Two-Node Cluster (Manual)":
- Node A has US data, Node B has EU data
- Both join the same gossip cluster
- Distributed queries return results from both nodes
"""

from conftest import wait_for


def _setup_two_nodes(node_factory):
    """Create a two-node cluster with US/EU order data.

    Returns (node_a, node_b).
    """
    node_a = node_factory()
    node_b = node_factory()

    # Node A: US data
    node_a.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region, random() * 100 as price "
        "FROM range(1000) t(i)"
    )
    node_a.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_a.flight_port})")
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_a.flight_port})"
    )

    # Node B: EU data, joins Node A
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'EU' as region, random() * 100 as price "
        "FROM range(1000) t(i)"
    )
    node_b.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})")
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{node_a.gossip_port}')"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )

    return node_a, node_b


def test_gossip_convergence(node_factory):
    """Both nodes see each other via trex_db_nodes() after gossip join."""
    node_a, node_b = _setup_two_nodes(node_factory)

    # Wait for both nodes to see 2 members
    wait_for(
        node_a,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )
    wait_for(
        node_b,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )


def test_swarm_tables_both_nodes(node_factory):
    """trex_db_tables() shows orders table from both nodes."""
    node_a, node_b = _setup_two_nodes(node_factory)

    # Wait for convergence first
    wait_for(
        node_a,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )

    tables = wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 2,
        timeout=10,
    )
    assert len(tables) >= 2, f"Expected orders from 2 nodes, got {len(tables)}"


def test_distributed_query_regions(node_factory):
    """Distributed query returns rows from both US and EU regions."""
    node_a, node_b = _setup_two_nodes(node_factory)

    # Wait for convergence
    wait_for(
        node_a,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )

    # Run distributed query
    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query("
        "'SELECT region, COUNT(*) as cnt FROM orders GROUP BY region')",
        lambda rows: len(rows) >= 2,
        timeout=10,
    )

    # Build a dict of region -> count
    region_counts = {row[0]: int(row[1]) for row in result}
    assert "US" in region_counts, f"Missing US region in {region_counts}"
    assert "EU" in region_counts, f"Missing EU region in {region_counts}"
    assert region_counts["US"] == 1000, f"US count {region_counts['US']} != 1000"
    assert region_counts["EU"] == 1000, f"EU count {region_counts['EU']} != 1000"
