import duckdb
import multiprocessing as mp
import pytest
import time
import os

# Must use 'spawn' so each child gets a fresh process (no inherited static state).
try:
    mp.set_start_method("spawn")
except RuntimeError:
    pass  # already set

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_EXT_TREX = f"{REPO_ROOT}/plugins/db/build/debug/extension/db/db.trex"
PGWIRE_EXT_TREX = f"{REPO_ROOT}/plugins/pgwire/build/debug/extension/pgwire/pgwire.trex"
ATLAS_EXT_TREX = f"{REPO_ROOT}/plugins/atlas/build/release/extension/circe/circe.trex"
AI_EXT_TREX = f"{REPO_ROOT}/plugins/ai/build/debug/extension/ai/ai.trex"
CHDB_EXT_TREX = f"{REPO_ROOT}/plugins/chdb/build/debug/extension/chdb/chdb.trex"
HANA_EXT_TREX = f"{REPO_ROOT}/plugins/hana/build/debug/extension/hana_scan/hana_scan.trex"
TPM_EXT_TREX = f"{REPO_ROOT}/plugins/tpm/build/debug/extension/tpm/tpm.trex"
ETL_EXT_TREX = f"{REPO_ROOT}/plugins/etl/build/debug/extension/etl/etl.trex"
MIGRATION_EXT_TREX = f"{REPO_ROOT}/plugins/migration/build/debug/extension/migration/migration.trex"
CQL2ELM_EXT_TREX = f"{REPO_ROOT}/plugins/cql2elm/build/release/extension/cql2elm/cql2elm.trex"
FHIR_EXT_TREX = f"{REPO_ROOT}/plugins/fhir/build/debug/extension/fhir/fhir.trex"

# trexsql Python API requires .duckdb_extension suffix for LOAD.
DB_EXT = f"{REPO_ROOT}/plugins/db/build/debug/extension/db/db.duckdb_extension"
PGWIRE_EXT = f"{REPO_ROOT}/plugins/pgwire/build/debug/extension/pgwire/pgwire.duckdb_extension"
ATLAS_EXT = f"{REPO_ROOT}/plugins/atlas/build/release/extension/circe/circe.duckdb_extension"
AI_EXT = f"{REPO_ROOT}/plugins/ai/build/debug/extension/ai/ai.duckdb_extension"
CHDB_EXT = f"{REPO_ROOT}/plugins/chdb/build/debug/extension/chdb/chdb.duckdb_extension"
HANA_EXT = f"{REPO_ROOT}/plugins/hana/build/debug/extension/hana_scan/hana_scan.duckdb_extension"
TPM_EXT = f"{REPO_ROOT}/plugins/tpm/build/debug/extension/tpm/tpm.duckdb_extension"
ETL_EXT = f"{REPO_ROOT}/plugins/etl/build/debug/extension/etl/etl.duckdb_extension"
MIGRATION_EXT = f"{REPO_ROOT}/plugins/migration/build/debug/extension/migration/migration.duckdb_extension"
CQL2ELM_EXT = f"{REPO_ROOT}/plugins/cql2elm/build/release/extension/cql2elm/cql2elm.duckdb_extension"
FHIR_EXT = f"{REPO_ROOT}/plugins/fhir/build/debug/extension/fhir/fhir.duckdb_extension"

for src, dst in [
    (DB_EXT_TREX, DB_EXT),
    (PGWIRE_EXT_TREX, PGWIRE_EXT),
    (ATLAS_EXT_TREX, ATLAS_EXT),
    (CQL2ELM_EXT_TREX, CQL2ELM_EXT),
    (AI_EXT_TREX, AI_EXT),
    (CHDB_EXT_TREX, CHDB_EXT),
    (HANA_EXT_TREX, HANA_EXT),
    (TPM_EXT_TREX, TPM_EXT),
    (ETL_EXT_TREX, ETL_EXT),
    (MIGRATION_EXT_TREX, MIGRATION_EXT),
    (FHIR_EXT_TREX, FHIR_EXT),
]:
    if os.path.exists(src) and not os.path.exists(dst):
        os.symlink(src, dst)

_next_gossip_port = 19000
_next_flight_port = 19100
_next_pgwire_port = 19200


def alloc_ports():
    """Allocate a unique gossip+flight+pgwire port tuple per call."""
    global _next_gossip_port, _next_flight_port, _next_pgwire_port
    gp, fp, pp = _next_gossip_port, _next_flight_port, _next_pgwire_port
    _next_gossip_port += 1
    _next_flight_port += 1
    _next_pgwire_port += 1
    return gp, fp, pp


# ---------------------------------------------------------------------------
# Node: each node is a separate OS process (extensions use static globals)
# ---------------------------------------------------------------------------

def _node_worker(ext_paths, cmd_queue, result_queue):
    """Child process: create trexsql connection, load extensions, run commands."""
    try:
        conn = duckdb.connect(":memory:", config={
            "allow_unsigned_extensions": "true",
            "allow_extensions_metadata_mismatch": "true",
        })
        for path in ext_paths:
            conn.execute(f"LOAD '{path}'")
        result_queue.put(("ready", None))
    except Exception as e:
        result_queue.put(("init_error", str(e)))
        return

    while True:
        cmd = cmd_queue.get()
        if cmd is None:  # shutdown
            break
        try:
            result = conn.execute(cmd)
            try:
                rows = result.fetchall()
            except Exception:
                rows = []
            result_queue.put(("ok", rows))
        except Exception as e:
            result_queue.put(("error", str(e)))

    conn.close()


class Node:
    """A trexsql node running in a separate process with extensions loaded."""

    def __init__(self, ext_paths, gossip_port, flight_port, pgwire_port):
        self.gossip_port = gossip_port
        self.flight_port = flight_port
        self.pgwire_port = pgwire_port
        self._cmd_queue = mp.Queue()
        self._result_queue = mp.Queue()
        self._process = mp.Process(
            target=_node_worker,
            args=(ext_paths, self._cmd_queue, self._result_queue),
        )
        self._process.start()
        status, data = self._result_queue.get(timeout=15)
        if status != "ready":
            self._process.join(timeout=5)
            raise RuntimeError(f"Node init failed: {data}")

    def execute(self, sql):
        """Execute SQL and return fetchall() result (list of tuples)."""
        self._cmd_queue.put(sql)
        status, data = self._result_queue.get(timeout=30)
        if status == "error":
            raise RuntimeError(data)
        return data

    def close(self):
        try:
            self._cmd_queue.put(None)
            self._process.join(timeout=5)
        except Exception:
            pass
        if self._process.is_alive():
            self._process.kill()
            self._process.join(timeout=2)


def wait_for(node, sql, check, timeout=10, interval=0.5):
    """Retry sql on node until check(result) returns True."""
    deadline = time.time() + timeout
    last_result = None
    last_err = None
    while time.time() < deadline:
        try:
            last_result = node.execute(sql)
            if check(last_result):
                return last_result
        except Exception as e:
            last_err = e
        time.sleep(interval)
    msg = f"Condition not met within {timeout}s for: {sql}"
    if last_result is not None:
        msg += f"\nLast result: {last_result}"
    if last_err is not None:
        msg += f"\nLast error: {last_err}"
    raise TimeoutError(msg)


@pytest.fixture
def node_factory():
    """Factory that creates trexsql nodes (each in a separate process)."""
    nodes = []

    def create_node(load_db=True, load_pgwire=False,
                     load_atlas=False, load_cql2elm=False, load_ai=False,
                     load_chdb=False, load_hana=False, load_tpm=False,
                     load_etl=False, load_migration=False, load_fhir=False):
        ext_paths = []
        if load_db:
            ext_paths.append(DB_EXT)
        if load_pgwire:
            ext_paths.append(PGWIRE_EXT)
        if load_atlas:
            ext_paths.append(ATLAS_EXT)
        if load_cql2elm:
            ext_paths.append(CQL2ELM_EXT)
        if load_ai:
            ext_paths.append(AI_EXT)
        if load_chdb:
            ext_paths.append(CHDB_EXT)
        if load_hana:
            ext_paths.append(HANA_EXT)
        if load_tpm:
            ext_paths.append(TPM_EXT)
        if load_etl:
            ext_paths.append(ETL_EXT)
        if load_migration:
            ext_paths.append(MIGRATION_EXT)
        if load_fhir:
            ext_paths.append(FHIR_EXT)
        gossip_port, flight_port, pgwire_port = alloc_ports()
        node = Node(ext_paths, gossip_port, flight_port, pgwire_port)
        nodes.append(node)
        return node

    yield create_node

    for node in nodes:
        try:
            node.close()
        except Exception:
            pass


def create_node_with_tables(node_factory_fn, tables_sql_list, node_name, cluster_id,
                            gossip_seeds=None, distributed_engine=False,
                            roles=None):
    """Create a node, start gossip + flight, and populate tables.

    Args:
        node_factory_fn: The create_node callable from node_factory fixture.
        tables_sql_list: List of SQL CREATE TABLE statements to execute.
        node_name: Logical name for this node in the cluster.
        cluster_id: Cluster identifier for gossip.
        gossip_seeds: Optional comma-separated seed addresses (host:port,...).
        distributed_engine: Whether to enable the DataFusion distributed engine.
        roles: List of distributed roles (e.g. ["scheduler", "executor"]).

    Returns:
        Node instance with tables created and gossip/flight started.
    """
    node = node_factory_fn()

    # Create tables
    for sql in tables_sql_list:
        node.execute(sql)

    # Start gossip
    if gossip_seeds:
        node.execute(
            f"SELECT trex_db_start_seeds('127.0.0.1', {node.gossip_port}, "
            f"'{cluster_id}', '{gossip_seeds}')"
        )
    else:
        node.execute(
            f"SELECT trex_db_start('127.0.0.1', {node.gossip_port}, '{cluster_id}')"
        )

    # Start flight server
    node.execute(
        f"SELECT trex_db_flight_start('0.0.0.0', {node.flight_port})"
    )

    return node


def setup_two_node_cluster(node_factory_fn, tables_a, tables_b,
                           cluster_id="test-cluster",
                           distributed_engine=False):
    """Set up a two-node cluster with different tables on each node.

    Args:
        node_factory_fn: The create_node callable from node_factory fixture.
        tables_a: List of SQL CREATE TABLE statements for node A.
        tables_b: List of SQL CREATE TABLE statements for node B.
        cluster_id: Cluster identifier.
        distributed_engine: Whether to enable distributed engine.

    Returns:
        Tuple of (node_a, node_b).
    """
    node_a = create_node_with_tables(
        node_factory_fn, tables_a, "node-a", cluster_id,
        distributed_engine=distributed_engine,
        roles=["scheduler", "executor"],
    )
    node_b = create_node_with_tables(
        node_factory_fn, tables_b, "node-b", cluster_id,
        gossip_seeds=f"127.0.0.1:{node_a.gossip_port}",
        distributed_engine=distributed_engine,
        roles=["executor"],
    )
    return node_a, node_b


def setup_three_node_cluster(node_factory_fn, tables_a, tables_b, tables_c,
                             cluster_id="test-cluster",
                             distributed_engine=False):
    """Set up a three-node cluster with different tables on each node.

    Returns:
        Tuple of (node_a, node_b, node_c).
    """
    node_a = create_node_with_tables(
        node_factory_fn, tables_a, "node-a", cluster_id,
        distributed_engine=distributed_engine,
        roles=["scheduler", "executor"],
    )
    seed = f"127.0.0.1:{node_a.gossip_port}"
    node_b = create_node_with_tables(
        node_factory_fn, tables_b, "node-b", cluster_id,
        gossip_seeds=seed,
        distributed_engine=distributed_engine,
        roles=["executor"],
    )
    node_c = create_node_with_tables(
        node_factory_fn, tables_c, "node-c", cluster_id,
        gossip_seeds=seed,
        distributed_engine=distributed_engine,
        roles=["executor"],
    )
    return node_a, node_b, node_c
