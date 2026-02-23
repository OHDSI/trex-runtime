"""Tier 7: Shuffle-based distributed join integration tests.

Exercises the shuffle optimizer and DoExchange-based hash partitioning
for cross-node joins where tables are on different nodes exclusively
(not co-located). The shuffle optimizer triggers when a HashJoinExec
has children on different nodes.

Set SWARM_BROADCAST_THRESHOLD=0 to force hash shuffle on small test data.

Critical ordering:
  1. Create tables on separate nodes (exclusive placement)
  2. Start flight + gossip + register flight service
  3. Wait for gossip + catalog convergence
  4. trex_db_set_distributed(true) on the scheduler node
  5. trex_db_query(sql) routes through DataFusion with shuffle
"""

from conftest import wait_for


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------

def _setup_exclusive_tables_two_nodes(node_factory):
    """Two nodes with tables on different nodes exclusively.

    Node A (scheduler): ONLY orders table - 500 rows
        id, customer_id = i % 50, amount = CAST(i * 10.0 AS DOUBLE)
    Node B (executor): ONLY customers table - 50 rows
        id, name = 'Customer_' || i

    Tables are on different nodes exclusively - ensures not co-located.

    Returns (scheduler, node_b).
    """
    scheduler = node_factory()
    node_b = node_factory()

    # Node A (scheduler): orders only
    scheduler.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, i % 50 AS customer_id, "
        "CAST(i * 10.0 AS DOUBLE) AS amount "
        "FROM range(500) t(i)"
    )
    scheduler.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {scheduler.flight_port})"
    )
    scheduler.execute(
        f"SELECT trex_db_start('0.0.0.0', {scheduler.gossip_port}, 'test-cluster')"
    )
    scheduler.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {scheduler.flight_port})"
    )

    # Node B: customers only
    node_b.execute(
        "CREATE TABLE customers AS "
        "SELECT i AS id, 'Customer_' || i AS name "
        "FROM range(50) t(i)"
    )
    node_b.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})"
    )
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'127.0.0.1:{scheduler.gossip_port}')"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )

    # Wait for gossip convergence
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )
    # Wait for catalog convergence (orders on A, customers on B = 2 entries)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 2,
        timeout=10,
    )

    # Enable distributed engine on scheduler
    scheduler.execute("SELECT trex_db_set_distributed(true)")

    return scheduler, node_b


def _setup_exclusive_tables_three_nodes(node_factory):
    """Three nodes with tables on different nodes exclusively.

    Node A (scheduler): orders - 500 rows
    Node B: customers - 50 rows
    Node C: products - 20 rows (id, name, price)

    Returns (scheduler, node_b, node_c).
    """
    scheduler = node_factory()
    node_b = node_factory()
    node_c = node_factory()

    # Node A (scheduler): orders only
    scheduler.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, i % 50 AS customer_id, "
        "i % 20 AS product_id, "
        "CAST(i * 10.0 AS DOUBLE) AS amount "
        "FROM range(500) t(i)"
    )
    scheduler.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {scheduler.flight_port})"
    )
    scheduler.execute(
        f"SELECT trex_db_start('0.0.0.0', {scheduler.gossip_port}, 'test-cluster')"
    )
    scheduler.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {scheduler.flight_port})"
    )

    seed = f"127.0.0.1:{scheduler.gossip_port}"

    # Node B: customers only
    node_b.execute(
        "CREATE TABLE customers AS "
        "SELECT i AS id, 'Customer_' || i AS name "
        "FROM range(50) t(i)"
    )
    node_b.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})"
    )
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, 'test-cluster', "
        f"'{seed}')"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )

    # Node C: products only
    node_c.execute(
        "CREATE TABLE products AS "
        "SELECT i AS id, 'Product_' || i AS name, "
        "CAST((i + 1) * 5.0 AS DOUBLE) AS price "
        "FROM range(20) t(i)"
    )
    node_c.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {node_c.flight_port})"
    )
    node_c.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_c.gossip_port}, 'test-cluster', "
        f"'{seed}')"
    )
    node_c.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_c.flight_port})"
    )

    # Wait for gossip convergence (3 nodes)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 3,
        timeout=20,
    )
    # Wait for catalog convergence (3 tables on 3 nodes)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 3,
        timeout=15,
    )

    # Enable distributed engine on scheduler
    scheduler.execute("SELECT trex_db_set_distributed(true)")

    return scheduler, node_b, node_c


# ---------------------------------------------------------------------------
# Cross-node join tests (2 nodes)
# ---------------------------------------------------------------------------

def test_shuffle_cross_node_join(node_factory):
    """Basic cross-node join: COUNT(*) = 500 (every order has a customer)."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT COUNT(*) AS cnt "
        "FROM orders o JOIN customers c ON o.customer_id = c.id')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=30,
    )
    assert int(result[0][0]) == 500, (
        f"Expected 500 joined rows, got {result[0][0]}"
    )


def test_shuffle_join_with_aggregation(node_factory):
    """Cross-node join with GROUP BY: 50 groups, each with count=10."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, COUNT(*) AS cnt, SUM(o.amount) AS total "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "GROUP BY c.name ORDER BY c.name')",
        lambda rows: len(rows) >= 2,
        timeout=30,
    )

    # 50 customers, each should appear
    assert len(result) == 50, (
        f"Expected 50 customer groups, got {len(result)}"
    )

    # Each customer has 10 orders (500 orders / 50 customers)
    for row in result:
        name = row[0]
        cnt = int(row[1])
        assert cnt == 10, (
            f"Expected count=10 for {name}, got {cnt}"
        )


def test_shuffle_join_with_filter(node_factory):
    """Cross-node join with WHERE filter on amount."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    # amount = i * 10.0, so amount > 2500 means i > 250 => 249 rows (251..499)
    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT COUNT(*) AS cnt "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "WHERE o.amount > 2500')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=30,
    )
    count = int(result[0][0])
    assert count == 249, (
        f"Expected 249 rows (orders with amount > 2500), got {count}"
    )


def test_shuffle_join_order_by_limit(node_factory):
    """Cross-node join with ORDER BY DESC LIMIT 10."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, o.amount "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "ORDER BY o.amount DESC LIMIT 10')",
        lambda rows: len(rows) == 10,
        timeout=30,
    )

    # Top 10 amounts: 4990, 4980, ..., 4900
    amounts = [float(row[1]) for row in result]
    expected = [4990.0, 4980.0, 4970.0, 4960.0, 4950.0,
                4940.0, 4930.0, 4920.0, 4910.0, 4900.0]
    assert amounts == expected, (
        f"Expected top 10 amounts {expected}, got {amounts}"
    )


def test_shuffle_empty_join_result(node_factory):
    """Cross-node join with impossible filter returns 0 rows."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT o.id, c.name "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "WHERE c.id > 9999')",
        lambda rows: isinstance(rows, list),
        timeout=30,
    )
    assert len(result) == 0, (
        f"Expected 0 rows, got {len(result)}"
    )


def test_shuffle_join_count_star(node_factory):
    """Cross-node join with GROUP BY + HAVING."""
    scheduler, _ = _setup_exclusive_tables_two_nodes(node_factory)

    # Each customer has exactly 10 orders, so HAVING COUNT(*) > 5 keeps all 50
    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, COUNT(*) AS cnt "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "GROUP BY c.name HAVING COUNT(*) > 5 "
        "ORDER BY c.name')",
        lambda rows: len(rows) >= 1,
        timeout=30,
    )

    assert len(result) == 50, (
        f"Expected 50 groups with count > 5, got {len(result)}"
    )

    for row in result:
        cnt = int(row[1])
        assert cnt > 5, (
            f"Expected count > 5 for {row[0]}, got {cnt}"
        )


# ---------------------------------------------------------------------------
# Three-node join test
# ---------------------------------------------------------------------------

def test_shuffle_three_node_join(node_factory):
    """Three-way cross-node join across 3 nodes."""
    scheduler, _, _ = _setup_exclusive_tables_three_nodes(node_factory)

    # orders JOIN customers ON customer_id = c.id
    # JOIN products ON product_id = p.id
    # 500 orders, each has a valid customer_id (0..49) and product_id (0..19)
    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT COUNT(*) AS cnt "
        "FROM orders o "
        "JOIN customers c ON o.customer_id = c.id "
        "JOIN products p ON o.product_id = p.id')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=45,
    )
    assert int(result[0][0]) == 500, (
        f"Expected 500 three-way joined rows, got {result[0][0]}"
    )
