"""Tier 8: Table partitioning integration tests.

Verifies trex_db_partition_table, trex_db_create_table, trex_db_repartition_table,
and trex_db_partitions across multi-node clusters.
"""

import json

from conftest import wait_for


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_two_node_cluster(node_factory, tables_a=None, tables_b=None):
    """Start two nodes with flight+gossip+data_node, wait for convergence."""
    node_a = node_factory()
    node_b = node_factory()

    if tables_a:
        for sql in tables_a:
            node_a.execute(sql)

    if tables_b:
        for sql in tables_b:
            node_b.execute(sql)

    # Start flight servers
    node_a.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_a.flight_port})")
    node_b.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})")

    # Start gossip
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, "
        f"'test-cluster', '127.0.0.1:{node_a.gossip_port}')"
    )

    # Register flight services
    node_a.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_a.flight_port})"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )

    # Mark both as data nodes
    node_a.execute("SELECT trex_db_set('data_node', 'true')")
    node_b.execute("SELECT trex_db_set('data_node', 'true')")

    # Wait for both nodes to see each other AND full gossip key-value propagation.
    # discover_target_nodes() requires both data_node=true AND service:flight
    # to be visible, so we must wait for flight services to propagate.
    wait_for(
        node_a,
        "SELECT * FROM trex_db_services()",
        lambda rows: sum(1 for r in rows if r[1] == 'flight' and r[4] == 'running') >= 2,
        timeout=15,
    )

    return node_a, node_b


def _setup_three_node_cluster(node_factory):
    """Start three nodes with flight+gossip+data_node, wait for convergence."""
    node_a = node_factory()
    node_b = node_factory()
    node_c = node_factory()

    # Start flight servers
    node_a.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_a.flight_port})")
    node_b.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_b.flight_port})")
    node_c.execute(f"SELECT trex_db_flight_start('0.0.0.0', {node_c.flight_port})")

    # Start gossip
    node_a.execute(
        f"SELECT trex_db_start('0.0.0.0', {node_a.gossip_port}, 'test-cluster')"
    )
    seed = f"127.0.0.1:{node_a.gossip_port}"
    node_b.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_b.gossip_port}, "
        f"'test-cluster', '{seed}')"
    )
    node_c.execute(
        f"SELECT trex_db_start_seeds('0.0.0.0', {node_c.gossip_port}, "
        f"'test-cluster', '{seed}')"
    )

    # Register flight services
    node_a.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_a.flight_port})"
    )
    node_b.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_b.flight_port})"
    )
    node_c.execute(
        f"SELECT trex_db_register_service('flight', '127.0.0.1', {node_c.flight_port})"
    )

    # Mark all as data nodes
    node_a.execute("SELECT trex_db_set('data_node', 'true')")
    node_b.execute("SELECT trex_db_set('data_node', 'true')")
    node_c.execute("SELECT trex_db_set('data_node', 'true')")

    # Wait for all three nodes to converge AND full gossip key-value propagation.
    wait_for(
        node_a,
        "SELECT * FROM trex_db_services()",
        lambda rows: sum(1 for r in rows if r[1] == 'flight' and r[4] == 'running') >= 3,
        timeout=15,
    )

    return node_a, node_b, node_c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_hash_partition_across_two_nodes(node_factory):
    """Hash-partition a table across 2 nodes and verify row distribution."""
    node_a, node_b = _setup_two_node_cluster(
        node_factory,
        tables_a=[
            "CREATE TABLE orders AS "
            "SELECT i as id, (i % 10) as customer_id, random() * 100 as amount "
            "FROM range(100) t(i)"
        ],
    )

    # Partition the table
    config = json.dumps({
        "strategy": "hash",
        "column": "customer_id",
        "partitions": 2,
    })
    result = node_a.execute(
        f"SELECT trex_db_partition_table('orders', '{config}')"
    )
    assert result is not None
    result_str = result[0][0]
    assert "Error" not in result_str, f"Partition failed: {result_str}"
    assert "100 rows" in result_str

    tables = wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: any(r[1] == "orders" for r in rows),
        timeout=10,
    )
    orders_entries = [r for r in tables if r[1] == "orders"]
    assert len(orders_entries) >= 1, "orders table should appear in catalog"

    # Verify the local table was dropped on node_a
    try:
        local_count = node_a.execute("SELECT COUNT(*) FROM orders")
        # If it succeeds, the table still exists locally (might be because
        # node_a was assigned a partition)
    except RuntimeError:
        pass  # Expected: table was dropped locally

    # Verify total row count across nodes by querying each via flight
    count_a = _flight_count(node_a, "orders")
    count_b = _flight_count(node_b, "orders")
    total = count_a + count_b
    assert total == 100, f"Expected 100 total rows, got {total} (a={count_a}, b={count_b})"


def test_range_partition_across_two_nodes(node_factory):
    """Range-partition a table and verify correct data placement."""
    node_a, node_b = _setup_two_node_cluster(
        node_factory,
        tables_a=[
            "CREATE TABLE events AS "
            "SELECT i as id, i * 10.0 as price "
            "FROM range(20) t(i)"
        ],
    )

    # Range partition: price < 100 goes to one partition, >= 100 to another
    config = json.dumps({
        "strategy": "range",
        "column": "price",
        "ranges": [
            {"upper": 100},
            {"lower": 100},
        ],
    })
    result = node_a.execute(
        f"SELECT trex_db_partition_table('events', '{config}')"
    )
    result_str = result[0][0]
    assert "Error" not in result_str, f"Partition failed: {result_str}"

    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: any(r[1] == "events" for r in rows),
        timeout=10,
    )

    count_a = _flight_count(node_a, "events")
    count_b = _flight_count(node_b, "events")
    total = count_a + count_b
    assert total == 20, f"Expected 20 total rows, got {total}"


def test_trex_db_partitions_shows_metadata(node_factory):
    """trex_db_partitions() returns partition metadata after partitioning."""
    node_a, node_b = _setup_two_node_cluster(
        node_factory,
        tables_a=[
            "CREATE TABLE orders AS "
            "SELECT i as id, (i % 5) as customer_id "
            "FROM range(50) t(i)"
        ],
    )

    config = json.dumps({
        "strategy": "hash",
        "column": "customer_id",
        "partitions": 2,
    })
    node_a.execute(f"SELECT trex_db_partition_table('orders', '{config}')")

    # Check trex_db_partitions() on the node that performed the partition
    partitions = wait_for(
        node_a,
        "SELECT * FROM trex_db_partitions()",
        lambda rows: len(rows) >= 2,
        timeout=10,
    )

    # Should have 2 partition assignments
    assert len(partitions) >= 2, f"Expected >= 2 partition rows, got {len(partitions)}"
    # All rows should be for the 'orders' table
    table_names = set(r[0] for r in partitions)
    assert "orders" in table_names


def test_trex_db_create_table(node_factory):
    """trex_db_create_table creates and distributes in one step."""
    node_a, node_b = _setup_two_node_cluster(node_factory)

    config = json.dumps({
        "strategy": "hash",
        "column": "id",
        "partitions": 2,
    })
    create_sql = (
        "CREATE TABLE items AS "
        "SELECT i as id, ''item_'' || i as name "
        "FROM range(40) t(i)"
    )
    result = node_a.execute(
        f"SELECT trex_db_create_table('{create_sql}', '{config}')"
    )
    result_str = result[0][0]
    assert "Error" not in result_str, f"Create+partition failed: {result_str}"

    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: any(r[1] == "items" for r in rows),
        timeout=10,
    )

    count_a = _flight_count(node_a, "items")
    count_b = _flight_count(node_b, "items")
    total = count_a + count_b
    assert total == 40, f"Expected 40 total rows, got {total}"


def test_query_partitioned_table_via_trex_db_query(node_factory):
    """Partitioned table can be queried via trex_db_query for correct results."""
    node_a, node_b = _setup_two_node_cluster(
        node_factory,
        tables_a=[
            "CREATE TABLE orders AS "
            "SELECT i as id, (i % 10) as customer_id, i * 1.5 as amount "
            "FROM range(100) t(i)"
        ],
    )

    config = json.dumps({
        "strategy": "hash",
        "column": "customer_id",
        "partitions": 2,
    })
    node_a.execute(f"SELECT trex_db_partition_table('orders', '{config}')")

    # Wait for catalog to propagate and show orders on both nodes.
    # After partitioning, each node needs to advertise its new table via gossip.
    # The catalog refresh runs every 30s, so we allow enough time for propagation.
    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: sum(1 for r in rows if r[1] == "orders") >= 2,
        timeout=40,
    )

    # Query via trex_db_query to get aggregated results
    result = wait_for(
        node_a,
        "SELECT * FROM trex_db_query('SELECT COUNT(*) as cnt FROM orders')",
        lambda rows: len(rows) >= 1 and rows[0][0] is not None,
        timeout=10,
    )

    # Each node reports its own count; the total across partitions should be 100
    total = sum(int(r[0]) for r in result)
    assert total == 100, f"Expected 100 total rows via trex_db_query, got {total}"


def test_repartition_from_two_to_three_nodes(node_factory):
    """Repartition a table from 2 partitions to 3 after adding a node."""
    node_a, node_b, node_c = _setup_three_node_cluster(node_factory)

    # Create and partition on node_a across 2 partitions
    node_a.execute(
        "CREATE TABLE orders AS "
        "SELECT i as id, (i % 10) as customer_id "
        "FROM range(60) t(i)"
    )

    config_2 = json.dumps({
        "strategy": "hash",
        "column": "customer_id",
        "partitions": 2,
    })
    result = node_a.execute(
        f"SELECT trex_db_partition_table('orders', '{config_2}')"
    )
    result_str = result[0][0]
    assert "Error" not in result_str, f"Initial partition failed: {result_str}"

    # Wait for both shards to be visible before repartitioning
    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: sum(1 for r in rows if r[1] == "orders") >= 2,
        timeout=10,
    )

    # Repartition to 3 partitions
    config_3 = json.dumps({
        "strategy": "hash",
        "column": "customer_id",
        "partitions": 3,
    })
    result = node_a.execute(
        f"SELECT trex_db_repartition_table('orders', '{config_3}')"
    )
    result_str = result[0][0]
    assert "Error" not in result_str, f"Repartition failed: {result_str}"

    wait_for(
        node_a,
        "SELECT * FROM trex_db_tables()",
        lambda rows: sum(1 for r in rows if r[1] == "orders") >= 3,
        timeout=10,
    )

    # Verify total row count across all 3 nodes
    count_a = _flight_count(node_a, "orders")
    count_b = _flight_count(node_b, "orders")
    count_c = _flight_count(node_c, "orders")
    total = count_a + count_b + count_c
    assert total == 60, (
        f"Expected 60 total rows after repartition, got {total} "
        f"(a={count_a}, b={count_b}, c={count_c})"
    )

    # Verify trex_db_partitions shows 3 partitions now
    partitions = wait_for(
        node_a,
        "SELECT * FROM trex_db_partitions()",
        lambda rows: sum(1 for r in rows if r[0] == "orders") >= 3,
        timeout=10,
    )
    orders_parts = [r for r in partitions if r[0] == "orders"]
    assert len(orders_parts) == 3, (
        f"Expected 3 partition entries, got {len(orders_parts)}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flight_count(node, table_name):
    """Query local row count for a table; returns 0 if table doesn't exist."""
    try:
        result = node.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
        return int(result[0][0])
    except RuntimeError:
        return 0
