"""Tier 3: Distributed aggregation tests.

Builds on the two-node cluster to verify that distributed aggregation
functions (COUNT, SUM, MIN, MAX, AVG) produce correct combined results.
"""

from conftest import wait_for


def _setup_two_nodes(node_factory):
    """Create a two-node cluster with deterministic price data.

    Node A: 1000 rows, region='US', price = i (0..999)
    Node B: 1000 rows, region='EU', price = i + 1000 (1000..1999)

    Returns (node_a, node_b).
    """
    node_a = node_factory()
    node_b = node_factory()

    # Node A: deterministic prices 0..999
    node_a.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'US' as region, CAST(i AS DOUBLE) as price "
        "FROM range(1000) t(i)"
    )
    node_a.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_a.flight_port})")
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_a.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_a.flight_port})"
    )

    # Node B: deterministic prices 1000..1999
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, 'EU' as region, CAST(i + 1000 AS DOUBLE) as price "
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

    # Wait for gossip convergence (both nodes see each other)
    wait_for(
        node_a,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )

    # Wait for catalog convergence (both nodes' tables visible)
    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )

    return node_a, node_b


def test_distributed_count(node_factory):
    """COUNT(*) across two nodes returns 2000."""
    node_a, _ = _setup_two_nodes(node_factory)

    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) as cnt FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )
    assert int(result[0][0]) == 2000


def test_distributed_sum(node_factory):
    """SUM(price) across two nodes returns combined sum.

    Node A: sum(0..999) = 499500
    Node B: sum(1000..1999) = 1499500
    Total: 1999000
    """
    node_a, _ = _setup_two_nodes(node_factory)

    expected_sum = sum(range(2000))  # 0+1+...+1999 = 1999000
    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query('SELECT SUM(price) as total FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )
    assert float(result[0][0]) == expected_sum, (
        f"SUM got {result[0][0]}, expected {expected_sum}"
    )


def test_distributed_min_max(node_factory):
    """MIN(price) = 0, MAX(price) = 1999 across both nodes."""
    node_a, _ = _setup_two_nodes(node_factory)

    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query("
        "'SELECT MIN(price) as min_p, MAX(price) as max_p FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )
    assert float(result[0][0]) == 0.0, f"MIN got {result[0][0]}, expected 0.0"
    assert float(result[0][1]) == 1999.0, f"MAX got {result[0][1]}, expected 1999.0"


def test_distributed_avg(node_factory):
    """AVG(price) across two nodes returns weighted average.

    avg(0..1999) = 999.5
    """
    node_a, _ = _setup_two_nodes(node_factory)

    expected_avg = sum(range(2000)) / 2000  # 999.5
    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query('SELECT AVG(price) as avg_p FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )
    assert abs(float(result[0][0]) - expected_avg) < 0.01, (
        f"AVG got {result[0][0]}, expected {expected_avg}"
    )
