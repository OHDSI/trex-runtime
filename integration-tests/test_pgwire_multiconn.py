"""PgWire multi-connection integration tests.

Verifies that the pgwire QueryExecutor thread pool correctly handles
concurrent queries from multiple clients. The executor clones a single
trexsql connection across N worker threads and dispatches incoming queries
via a shared channel — these tests exercise that path.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2


def _connect(port, password="test"):
    return psycopg2.connect(
        host="127.0.0.1",
        port=port,
        user="any",
        password=password,
        dbname="memory",
    )


def _start_pgwire(node):
    node.execute(
        f"SELECT trex_pgwire_start('127.0.0.1', {node.pgwire_port}, 'test', '')"
    )


def _stop_pgwire(node):
    node.execute(
        f"SELECT trex_pgwire_stop('127.0.0.1', {node.pgwire_port})"
    )


def test_parallel_queries_on_thread_pool(node_factory):
    """Queries from multiple clients are dispatched across executor worker threads."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_pgwire(node)

    num_clients = 8
    barrier = threading.Barrier(num_clients, timeout=10)

    def run_query(idx):
        conn = _connect(node.pgwire_port)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            # barrier ensures all clients submit at the same time,
            # so queries land on different worker threads
            barrier.wait()
            cur.execute(f"SELECT {idx}")
            rows = cur.fetchall()
            cur.close()
            return idx, rows[0][0]
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=num_clients) as pool:
        futures = {pool.submit(run_query, i): i for i in range(num_clients)}
        results = {}
        for f in as_completed(futures):
            idx, val = f.result()
            results[idx] = val

    for i in range(num_clients):
        assert results[i] == i, f"client {i}: expected {i}, got {results[i]}"

    _stop_pgwire(node)


def test_cloned_connections_share_data(node_factory):
    """All executor worker threads (cloned connections) see the same tables."""
    node = node_factory(load_pgwire=True, load_db=False)
    node.execute(
        "CREATE TABLE shared AS SELECT i AS id FROM range(500) t(i)"
    )
    _start_pgwire(node)

    num_clients = 6
    barrier = threading.Barrier(num_clients, timeout=10)

    def read_count(_idx):
        conn = _connect(node.pgwire_port)
        try:
            cur = conn.cursor()
            barrier.wait()
            cur.execute("SELECT COUNT(*) FROM shared")
            count = cur.fetchall()[0][0]
            cur.close()
            return count
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=num_clients) as pool:
        counts = list(pool.map(read_count, range(num_clients)))

    assert all(c == 500 for c in counts), f"expected all 500, got {counts}"

    _stop_pgwire(node)


def test_more_clients_than_workers(node_factory):
    """More concurrent clients than pool workers — queries queue and all complete."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_pgwire(node)

    # the executor pool is small (typically 4 workers),
    # send 20 concurrent queries to exercise channel queuing
    num_clients = 20

    def run_query(idx):
        conn = _connect(node.pgwire_port)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"SELECT {idx} * {idx}")
            val = cur.fetchall()[0][0]
            cur.close()
            return idx, val
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=num_clients) as pool:
        futures = {pool.submit(run_query, i): i for i in range(num_clients)}
        results = {}
        for f in as_completed(futures):
            idx, val = f.result()
            results[idx] = val

    for i in range(num_clients):
        assert results[i] == i * i, f"client {i}: expected {i*i}, got {results[i]}"

    _stop_pgwire(node)


def test_concurrent_writes_visible_across_workers(node_factory):
    """A write on one worker thread is visible to reads on other workers."""
    node = node_factory(load_pgwire=True, load_db=False)
    node.execute("CREATE TABLE counter (val INTEGER)")
    _start_pgwire(node)

    barrier = threading.Barrier(2, timeout=10)
    writer_done = threading.Event()
    reader_counts = []

    def writer():
        conn = _connect(node.pgwire_port)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            barrier.wait()
            for i in range(50):
                cur.execute(f"INSERT INTO counter VALUES ({i})")
            cur.close()
        finally:
            conn.close()
            writer_done.set()

    def reader():
        conn = _connect(node.pgwire_port)
        try:
            cur = conn.cursor()
            barrier.wait()
            while not writer_done.is_set():
                cur.execute("SELECT COUNT(*) FROM counter")
                reader_counts.append(cur.fetchall()[0][0])
            # final read after writer finishes
            cur.execute("SELECT COUNT(*) FROM counter")
            reader_counts.append(cur.fetchall()[0][0])
            cur.close()
        finally:
            conn.close()

    threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    # final read must see all 50 rows
    assert reader_counts[-1] == 50, f"final count {reader_counts[-1]}"
    # monotonically non-decreasing (workers share same db)
    for a, b in zip(reader_counts, reader_counts[1:]):
        assert a <= b, f"not monotonic: {reader_counts}"

    _stop_pgwire(node)


def test_concurrent_mixed_query_types(node_factory):
    """Different query types (aggregation, filter, scan) run in parallel."""
    node = node_factory(load_pgwire=True, load_db=False)
    node.execute(
        "CREATE TABLE products AS "
        "SELECT i AS id, (i * 9.99)::DOUBLE AS price FROM range(100) t(i)"
    )
    _start_pgwire(node)

    barrier = threading.Barrier(4, timeout=10)
    results = {}

    def query(name, sql):
        conn = _connect(node.pgwire_port)
        try:
            cur = conn.cursor()
            barrier.wait()
            cur.execute(sql)
            results[name] = cur.fetchall()[0][0]
            cur.close()
        finally:
            conn.close()

    threads = [
        threading.Thread(target=query, args=("count", "SELECT COUNT(*) FROM products")),
        threading.Thread(target=query, args=("max_id", "SELECT MAX(id) FROM products")),
        threading.Thread(target=query, args=("filtered", "SELECT COUNT(*) FROM products WHERE id >= 50")),
        threading.Thread(target=query, args=("sum_price", "SELECT SUM(price) FROM products")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert results["count"] == 100
    assert results["max_id"] == 99
    assert results["filtered"] == 50

    _stop_pgwire(node)


def test_rapid_connect_disconnect(node_factory):
    """Rapid open-query-close cycles don't leak or crash the executor."""
    node = node_factory(load_pgwire=True, load_db=False)
    _start_pgwire(node)

    for i in range(20):
        conn = _connect(node.pgwire_port)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"SELECT {i}")
            assert cur.fetchall() == [(i,)]
            cur.close()
        finally:
            conn.close()

    # executor still healthy after rapid cycling
    conn = _connect(node.pgwire_port)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 'alive'")
        assert cur.fetchall() == [("alive",)]
        cur.close()
    finally:
        conn.close()

    _stop_pgwire(node)
