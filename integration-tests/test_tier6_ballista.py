"""Tier 6: DataFusion distributed engine integration tests.

Exercises the DataFusion distributed engine end-to-end:
sharded table scans, aggregations, cross-node joins, and complex queries.

Unlike tiers 2-4 (which use the legacy coordinator path with
distributed_engine=False), these tests enable the DataFusion distributed
engine via trex_db_set_distributed(true).  This routes queries through
DataFusion planning and execution with DistributedExec for sharded tables
and Flight-based fan-out across nodes.

Critical ordering:
  1. Create tables + start flight + start gossip + register flight service
  2. Wait for gossip + catalog convergence (all nodes see all tables)
  3. trex_db_set_distributed(true) on the scheduler node
  4. trex_db_query(sql) routes through DataFusion
"""

from conftest import wait_for


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------

def _setup_two_nodes_sharded(node_factory):
    """Two nodes with sharded orders table (same schema, different data).

    Node A: orders id 0..999, region='US', amount=CAST(i AS DOUBLE)
    Node B: orders id 1000..1999, region='EU', amount=CAST(i+1000 AS DOUBLE)

    Classified as Sharded (2 partitions).

    Returns (scheduler, node_b).
    """
    scheduler = node_factory()
    node_b = node_factory()

    # Node A (scheduler): orders 0..999
    scheduler.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, 'US' AS region, CAST(i AS DOUBLE) AS amount "
        "FROM range(1000) t(i)"
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

    # Node B: orders 1000..1999
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i + 1000 AS id, 'EU' AS region, CAST(i + 1000 AS DOUBLE) AS amount "
        "FROM range(1000) t(i)"
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
    # Wait for catalog convergence (orders on 2 nodes)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 2,
        timeout=10,
    )

    # Enable distributed engine on scheduler
    scheduler.execute("SELECT trex_db_set_distributed(true)")

    return scheduler, node_b


def _setup_two_nodes_both_tables(node_factory):
    """Two nodes with both customers AND orders tables (both sharded).

    Node A: customers 0..9, orders 0..14 (customer_id = i % 10, amount = i * 10.0)
    Node B: customers 10..19, orders 15..29 (customer_id = (i%10)+10, amount = i * 10.0)

    Returns (scheduler, node_b).
    """
    scheduler = node_factory()
    node_b = node_factory()

    # Node A (scheduler)
    scheduler.execute(
        "CREATE TABLE customers AS "
        "SELECT i AS id, 'Customer_' || i AS name "
        "FROM range(10) t(i)"
    )
    scheduler.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, i % 10 AS customer_id, CAST(i * 10 AS DOUBLE) AS amount "
        "FROM range(15) t(i)"
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

    # Node B
    node_b.execute(
        "CREATE TABLE customers AS "
        "SELECT i + 10 AS id, 'Customer_' || (i + 10) AS name "
        "FROM range(10) t(i)"
    )
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i + 15 AS id, (i % 10) + 10 AS customer_id, "
        "CAST((i + 15) * 10 AS DOUBLE) AS amount "
        "FROM range(15) t(i)"
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
    # Wait for catalog convergence (2 tables x 2 nodes = 4 entries)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 4,
        timeout=10,
    )

    # Enable distributed engine on scheduler
    scheduler.execute("SELECT trex_db_set_distributed(true)")

    return scheduler, node_b


def _setup_three_nodes_sharded(node_factory):
    """Three nodes with sharded orders table (3 partitions).

    Node A: orders 0..999, region='US', amount=i
    Node B: orders 1000..1999, region='EU', amount=i+1000
    Node C: orders 2000..2999, region='APAC', amount=i+2000

    Returns (scheduler, node_b, node_c).
    """
    scheduler = node_factory()
    node_b = node_factory()
    node_c = node_factory()

    # Node A (scheduler)
    scheduler.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, 'US' AS region, CAST(i AS DOUBLE) AS amount "
        "FROM range(1000) t(i)"
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

    # Node B
    node_b.execute(
        "CREATE TABLE orders AS "
        "SELECT i + 1000 AS id, 'EU' AS region, CAST(i + 1000 AS DOUBLE) AS amount "
        "FROM range(1000) t(i)"
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

    # Node C
    node_c.execute(
        "CREATE TABLE orders AS "
        "SELECT i + 2000 AS id, 'APAC' AS region, CAST(i + 2000 AS DOUBLE) AS amount "
        "FROM range(1000) t(i)"
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
    # Wait for catalog convergence (orders on 3 nodes)
    wait_for(
        scheduler,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 3,
        timeout=15,
    )

    # Enable distributed engine on scheduler
    scheduler.execute("SELECT trex_db_set_distributed(true)")

    return scheduler, node_b, node_c


def _setup_single_node_distributed(node_factory):
    """Single node with customers + orders, distributed engine enabled.

    customers: id 0..9, name 'Customer_0'..'Customer_9'
    orders: id 0..29, customer_id = i % 10, amount = i * 10.0

    Returns node (scheduler).
    """
    node = node_factory()

    node.execute(
        "CREATE TABLE customers AS "
        "SELECT i AS id, 'Customer_' || i AS name "
        "FROM range(10) t(i)"
    )
    node.execute(
        "CREATE TABLE orders AS "
        "SELECT i AS id, i % 10 AS customer_id, CAST(i * 10 AS DOUBLE) AS amount "
        "FROM range(30) t(i)"
    )
    node.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node.flight_port})")
    node.execute(
        f"SELECT trex_db_start('0.0.0.0', {node.gossip_port}, 'test-cluster')"
    )
    node.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node.flight_port})"
    )

    # Wait for self-discovery
    wait_for(
        node,
        "SELECT * FROM trex_db_nodes()",
        lambda rows: len(rows) >= 1,
        timeout=10,
    )
    wait_for(
        node,
        "SELECT * FROM trex_db_tables()",
        lambda rows: len(rows) >= 2,  # customers + orders
        timeout=10,
    )

    # Enable distributed engine
    node.execute("SELECT trex_db_set_distributed(true)")

    return node


# ---------------------------------------------------------------------------
# Sharded table scans (2-node)
# ---------------------------------------------------------------------------

def test_distributed_sharded_count(node_factory):
    """COUNT(*) across two sharded partitions returns 2000."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) AS cnt FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    assert int(result[0][0]) == 2000


def test_distributed_sharded_aggregation(node_factory):
    """SUM, MIN, MAX, AVG across two sharded partitions."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT SUM(amount) AS s, MIN(amount) AS mn, "
        "MAX(amount) AS mx, AVG(amount) AS av FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    s = float(result[0][0])
    mn = float(result[0][1])
    mx = float(result[0][2])
    av = float(result[0][3])

    # sum(0..1999) = 1999000
    assert s == 1999000.0, f"Expected SUM=1999000, got {s}"
    assert mn == 0.0, f"Expected MIN=0, got {mn}"
    assert mx == 1999.0, f"Expected MAX=1999, got {mx}"
    assert abs(av - 999.5) < 0.01, f"Expected AVG=999.5, got {av}"


def test_distributed_sharded_group_by(node_factory):
    """GROUP BY region across two sharded partitions."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT region, COUNT(*) AS cnt, SUM(amount) AS s "
        "FROM orders GROUP BY region ORDER BY region')",
        lambda rows: len(rows) >= 2,
        timeout=15,
    )

    # Results ordered by region: EU, US
    assert result[0][0] == "EU"
    assert int(result[0][1]) == 1000
    assert float(result[0][2]) == 1499500.0  # sum(1000..1999)

    assert result[1][0] == "US"
    assert int(result[1][1]) == 1000
    assert float(result[1][2]) == 499500.0  # sum(0..999)


def test_distributed_filter_pushdown(node_factory):
    """Filter pushdown: amount > 1500 returns 499 rows (1501..1999)."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT COUNT(*) AS cnt FROM orders WHERE amount > 1500')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    assert int(result[0][0]) == 499


def test_distributed_filter_region(node_factory):
    """Filter by region='US' returns 1000 rows."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT COUNT(*) AS cnt FROM orders WHERE region = ''US''')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    assert int(result[0][0]) == 1000


def test_distributed_order_by_limit(node_factory):
    """ORDER BY amount DESC LIMIT 5 returns top 5 values."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 5')",
        lambda rows: len(rows) == 5,
        timeout=15,
    )

    ids = [int(row[0]) for row in result]
    amounts = [float(row[1]) for row in result]

    assert ids == [1999, 1998, 1997, 1996, 1995], f"Expected [1999..1995], got {ids}"
    assert amounts == [1999.0, 1998.0, 1997.0, 1996.0, 1995.0], (
        f"Expected [1999..1995], got {amounts}"
    )


def test_distributed_distinct(node_factory):
    """DISTINCT region returns EU, US (ordered)."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT DISTINCT region FROM orders ORDER BY region')",
        lambda rows: len(rows) == 2,
        timeout=15,
    )

    regions = [row[0] for row in result]
    assert regions == ["EU", "US"], f"Expected ['EU', 'US'], got {regions}"


def test_distributed_empty_result(node_factory):
    """Query with impossible filter returns 0 rows."""
    scheduler, _ = _setup_two_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT * FROM orders WHERE amount > 99999')",
        lambda rows: isinstance(rows, list),
        timeout=15,
    )
    assert len(result) == 0, f"Expected 0 rows, got {len(result)}"


# ---------------------------------------------------------------------------
# Joins (2-node, both tables sharded)
# ---------------------------------------------------------------------------

def test_distributed_cross_shard_join(node_factory):
    """Cross-shard join: customers JOIN orders across two partitions."""
    scheduler, _ = _setup_two_nodes_both_tables(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, o.amount "
        "FROM customers c JOIN orders o ON c.id = o.customer_id "
        "ORDER BY c.id, o.id')",
        lambda rows: len(rows) >= 2,
        timeout=20,
    )

    # Both partitions contribute results.
    customer_ids = sorted(set(
        int(row[0].split("_")[1]) for row in result
    ))
    assert any(cid < 10 for cid in customer_ids), (
        "Missing customer IDs from node A (0-9)"
    )
    assert any(cid >= 10 for cid in customer_ids), (
        "Missing customer IDs from node B (10-19)"
    )

    # Total rows: Node A has 15 orders with customer_id 0..9 (all match),
    # Node B has 15 orders with customer_id 10..19 (all match) = 30 total.
    assert len(result) == 30, f"Expected 30 joined rows, got {len(result)}"


def test_distributed_join_with_aggregation(node_factory):
    """Join with GROUP BY: per-customer order count and sum."""
    scheduler, _ = _setup_two_nodes_both_tables(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, COUNT(o.id) AS cnt, SUM(o.amount) AS total "
        "FROM customers c JOIN orders o ON c.id = o.customer_id "
        "GROUP BY c.name ORDER BY c.name')",
        lambda rows: len(rows) >= 2,
        timeout=20,
    )

    # 20 customers (0-19), each with at least one order.
    assert len(result) == 20, f"Expected 20 customer groups, got {len(result)}"

    for row in result:
        name = row[0]
        cnt = int(row[1])
        total = float(row[2])
        assert name.startswith("Customer_"), f"Unexpected name: {name}"
        assert cnt > 0, f"Expected positive count for {name}, got {cnt}"
        assert total > 0, f"Expected positive total for {name}, got {total}"


def test_distributed_complex_query(node_factory):
    """WHERE + JOIN + GROUP BY + HAVING + ORDER BY + LIMIT."""
    scheduler, _ = _setup_two_nodes_both_tables(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT c.name, COUNT(o.id) AS cnt, SUM(o.amount) AS total "
        "FROM customers c "
        "JOIN orders o ON c.id = o.customer_id "
        "WHERE o.amount > 50 "
        "GROUP BY c.name "
        "HAVING COUNT(o.id) >= 1 "
        "ORDER BY total DESC "
        "LIMIT 10')",
        lambda rows: len(rows) >= 1,
        timeout=20,
    )

    # Verify ORDER BY DESC: totals should be non-increasing.
    totals = [float(row[2]) for row in result]
    for i in range(1, len(totals)):
        assert totals[i] <= totals[i - 1], (
            f"Results not sorted DESC: {totals[i-1]} followed by {totals[i]}"
        )

    # Verify LIMIT: at most 10 rows.
    assert len(result) <= 10, f"Expected at most 10 rows (LIMIT 10), got {len(result)}"

    # Verify HAVING: all groups have at least 1 order.
    for row in result:
        cnt = int(row[1])
        assert cnt >= 1, f"Expected cnt >= 1 (HAVING), got {cnt} for {row[0]}"

    # Verify WHERE: totals come from orders with amount > 50.
    for row in result:
        total = float(row[2])
        assert total > 50, f"Expected total > 50, got {total} for {row[0]}"


# ---------------------------------------------------------------------------
# Three-node cluster
# ---------------------------------------------------------------------------

def test_distributed_three_node_scan(node_factory):
    """Three-node sharded scan: GROUP BY region returns APAC, EU, US."""
    scheduler, _, _ = _setup_three_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query("
        "'SELECT region, COUNT(*) AS cnt "
        "FROM orders GROUP BY region ORDER BY region')",
        lambda rows: len(rows) == 3,
        timeout=20,
    )

    assert result[0][0] == "APAC"
    assert int(result[0][1]) == 1000

    assert result[1][0] == "EU"
    assert int(result[1][1]) == 1000

    assert result[2][0] == "US"
    assert int(result[2][1]) == 1000


def test_distributed_three_node_aggregation(node_factory):
    """Three-node sharded SUM: sum(0..2999) = 4498500."""
    scheduler, _, _ = _setup_three_nodes_sharded(node_factory)

    result = wait_for(
        scheduler,
        "SELECT * FROM trex_db_query('SELECT SUM(amount) AS s FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=20,
    )
    s = float(result[0][0])
    assert s == 4498500.0, f"Expected SUM=4498500, got {s}"


# ---------------------------------------------------------------------------
# Feature flag toggle
# ---------------------------------------------------------------------------

def test_distributed_feature_flag_toggle(node_factory):
    """Enable distributed -> query succeeds -> disable -> legacy succeeds -> re-enable -> succeeds."""
    node = _setup_single_node_distributed(node_factory)

    # Distributed is already enabled by _setup_single_node_distributed.
    # Query should succeed via DataFusion path.
    result = wait_for(
        node,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) AS cnt FROM customers')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    assert int(result[0][0]) == 10, (
        f"Distributed path: expected 10 customers, got {result[0][0]}"
    )

    # Disable distributed -> switch to legacy coordinator.
    node.execute("SELECT trex_db_set_distributed(false)")

    legacy_result = wait_for(
        node,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) AS cnt FROM customers')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )
    assert int(legacy_result[0][0]) == 10, (
        f"Legacy path: expected 10 customers, got {legacy_result[0][0]}"
    )

    # Re-enable distributed -> query should succeed again.
    node.execute("SELECT trex_db_set_distributed(true)")

    result2 = wait_for(
        node,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) AS cnt FROM customers')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=15,
    )
    assert int(result2[0][0]) == 10, (
        f"Re-enabled distributed: expected 10 customers, got {result2[0][0]}"
    )
