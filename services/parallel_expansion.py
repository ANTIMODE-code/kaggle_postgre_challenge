"""Resumable, parallel execution of a cross-table expansion.

The recipes are executed exactly as written. The only thing that changes is the
row subset substituted for ``{{source}}``: the protected Raw scope filter is kept
and a ``raw_id`` range is added, so every block reads a disjoint slice of the same
source.

Execution model
---------------
1. Plan transaction (REPEATABLE READ, destinations locked as in the single
   transaction mode): source manifest, the run record, the destinations whose
   recipes read the whole source (for example ``SELECT DISTINCT`` dimensions) and
   the block plan are committed together.
2. Block transactions, executed by several PostgreSQL sessions at once: each
   block runs the per-row recipes for its ``raw_id`` range, updates the managed
   row counters and marks itself done in the same transaction. A block that is
   already done is skipped, so a resumed run never inserts a row twice.
3. Finalization transaction: every block must be done and the source manifest
   must still match the one captured in step 1.

Versions are registered afterwards by the existing atomic version batch, with
the run identifier stored in the dependency payload. An interrupted run is
resumed by starting Expand again with the same source, scope and recipes.
"""

import hashlib
import json
import os
import re
import shlex
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from utils import sql_ident, sql_literal

STATE_SCHEMA = "pgdm_expansion"
RUNS_TABLE = f"{sql_ident(STATE_SCHEMA)}.{sql_ident('runs')}"
BLOCKS_TABLE = f"{sql_ident(STATE_SCHEMA)}.{sql_ident('blocks')}"
MANIFEST_TEMP_TABLE = "pg_temp.pgdm_cross_table_hash_manifest"

DEFAULT_BLOCK_ROWS = 500_000
MAX_DEFAULT_WORKERS = 8


def single_transaction_mode() -> bool:
    """PDM_EXPANSION_MODE=single_transaction keeps the original one-transaction Expand."""
    return str(os.getenv("PDM_EXPANSION_MODE") or "").strip().lower() == "single_transaction"


def configured_workers() -> int | None:
    """Explicit worker count from PDM_EXPANSION_WORKERS, or None for automatic."""
    value = str(os.getenv("PDM_EXPANSION_WORKERS") or "").strip()
    if not value:
        return None
    workers = int(value)
    if workers < 1:
        raise ValueError("PDM_EXPANSION_WORKERS must be at least 1.")
    return workers


def configured_block_rows() -> int:
    value = str(os.getenv("PDM_EXPANSION_BLOCK_ROWS") or "").strip()
    block_rows = int(value) if value else DEFAULT_BLOCK_ROWS
    if block_rows < 1:
        raise ValueError("PDM_EXPANSION_BLOCK_ROWS must be at least 1.")
    return block_rows


def state_ddl_sql() -> str:
    return f"""
CREATE SCHEMA IF NOT EXISTS {sql_ident(STATE_SCHEMA)};

CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint text NOT NULL,
    source_schema_name text NOT NULL,
    source_table_name text NOT NULL,
    raw_schemas text[] NOT NULL,
    destination_names text[] NOT NULL,
    raw_hash_manifest jsonb NOT NULL,
    expected_row_count bigint NOT NULL,
    whole_source_rows jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    status text NOT NULL CHECK (status IN ('running', 'blocks_done', 'completed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_single_open_run
    ON {RUNS_TABLE} ((true))
    WHERE status <> 'completed';

CREATE TABLE IF NOT EXISTS {BLOCKS_TABLE} (
    run_id bigint NOT NULL REFERENCES {RUNS_TABLE} (run_id) ON DELETE CASCADE,
    block_number integer NOT NULL,
    raw_id_from bigint NOT NULL,
    raw_id_to bigint NOT NULL CHECK (raw_id_to >= raw_id_from),
    source_rows bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'done')),
    destination_rows jsonb,
    started_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (run_id, block_number)
);

COMMENT ON SCHEMA {sql_ident(STATE_SCHEMA)} IS
    'Checkpoint state of resumable cross-table expansions (PGDM DB Manager).';
""".strip()


def expansion_fingerprint(source_schema_name, source_table_name, raw_schemas, destinations) -> str:
    payload = {
        "source": [source_schema_name, source_table_name],
        "raw_schemas": list(raw_schemas),
        "destinations": [
            [item["table_name"], list(item["statements"])]
            for item in destinations
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def reads_whole_source(statement: str) -> bool:
    """True when a recipe must see the complete source at once.

    ``SELECT DISTINCT`` and aggregates collapse rows across the whole source, so
    splitting them by raw_id would change the result (duplicates across blocks).
    Such recipes run once, unsplit, in the plan transaction.
    """
    normalized = re.sub(r"\s+", " ", statement.upper())
    return bool(
        re.search(r"\bSELECT DISTINCT\b", normalized)
        or re.search(r"\bGROUP BY\b", normalized)
        or re.search(r"\b(COUNT|SUM|AVG|MIN|MAX|STRING_AGG|ARRAY_AGG|JSON_AGG|JSONB_AGG|BOOL_AND|BOOL_OR)\s*\(", normalized)
        or re.search(r"\bOVER\s*\(", normalized)
        or re.search(r"\b(LIMIT|OFFSET|FETCH FIRST|UNION|INTERSECT|EXCEPT)\b", normalized)
    )


def split_destinations(destinations, scope_filter: str):
    """Whole-source destinations first, keeping the recipe order inside each group.

    Every whole-source destination must come before every per-block destination
    in the original order; otherwise a dimension could read rows that a later
    per-row recipe has not inserted yet, and the split would change the result.
    """
    whole_source, per_block = [], []
    for destination in destinations:
        if any(
            # A recipe that reads the source more than once (self-join) is not
            # row-local either: narrowing both sides to one block would change it.
            reads_whole_source(statement) or statement.count(scope_filter) != 1
            for statement in destination["statements"]
        ):
            if per_block:
                raise RuntimeError(
                    f"{destination['table_name']} aggregates the whole source but comes "
                    "after a per-row destination; parallel expansion would change its "
                    "result. Run it with PDM_EXPANSION_WORKERS=1."
                )
            whole_source.append(destination)
        else:
            per_block.append(destination)
    return whole_source, per_block


class ParallelExpansionRunner:
    def __init__(
        self,
        service,
        database_name,
        prepared,
        manifest_sql,
        lock_sql,
        workers,
        block_rows,
        cancel_event=None,
        progress_callback=None,
        timing_report=None,
    ):
        self.service = service
        self.timing_report = timing_report
        self.database_name = database_name
        self.prepared = prepared
        self.manifest_sql = manifest_sql
        self.lock_sql = lock_sql
        self.workers = workers
        self.block_rows = block_rows
        self.cancel_event = cancel_event
        self.progress_callback = progress_callback
        self.whole_source, self.per_block = split_destinations(
            prepared["destinations"], self._scope_filter()
        )
        self.fingerprint = expansion_fingerprint(
            prepared["source_schema_name"],
            prepared["source_table_name"],
            prepared["raw_schemas"],
            prepared["destinations"],
        )
        self.server_timings = []

    # ------------------------------------------------------------------ helpers
    def _psql(
        self,
        sql: str,
        database_name: str | None = None,
        timing_name: str | None = None,
        envelope: bool = False,
    ) -> str:
        """Run one psql batch on the server.

        Bookkeeping batches are timed by the server around the command and count
        in the measured total. Envelope batches report their own PostgreSQL
        clock_timestamp() steps, so the command around them is diagnostic only
        (as the single-transaction mode does) and is not counted twice. Block
        batches run on worker threads without a timing scope; their wall time is
        reported once, from the first block start to the last block commit.
        """
        if timing_name:
            with self.service.expansion_timing_scope(
                self.timing_report,
                "execute",
                timing_name,
                record_local_cpu=False,
                remote_include_in_total=not envelope,
            ):
                return self._psql(sql, database_name)
        command = (
            f"psql -h localhost -U {shlex.quote(self.service.sql_username)} "
            f"-d {shlex.quote(database_name or self.database_name)} "
            "-X -qAt -v ON_ERROR_STOP=1 -f -"
        )
        return self.service.run_remote_command(
            command,
            stdin_text=sql,
            cancel_event=self.cancel_event,
        )

    @staticmethod
    def _json_lines(output: str, key: str):
        found = []
        for line in (output or "").splitlines():
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and key in parsed:
                found.append(parsed)
        return found

    def _progress(self, progress, message):
        self.service._notify_progress(self.progress_callback, "execute", progress, message)

    def _scope_sql(self) -> str:
        return ", ".join(sql_literal(value) for value in self.prepared["raw_schemas"])

    def _source_sql(self) -> str:
        return (
            f"{sql_ident(self.prepared['source_schema_name'])}."
            f"{sql_ident(self.prepared['source_table_name'])}"
        )

    def _scope_filter(self) -> str:
        # Exactly the filter written by _prepare_cross_table_expansion_statement
        # inside the protected source subquery.
        return f"WHERE {sql_ident('raw_schema')} IN ({self._scope_sql()})"

    def _block_statement(self, statement: str, raw_id_from: int, raw_id_to: int) -> str:
        # Narrow the protected scope subquery to the block's raw_id range.
        scope_filter = self._scope_filter()
        if statement.count(scope_filter) != 1:
            raise RuntimeError("Could not locate the protected source scope in a recipe.")
        return statement.replace(
            scope_filter,
            f"{scope_filter}\n      AND {sql_ident('raw_id')} BETWEEN {int(raw_id_from)} AND {int(raw_id_to)}",
        )

    @staticmethod
    def _counter_sql(table_rows) -> str:
        from services.postgres_service import DATA_TABLE_ROW_COUNTS

        parts = []
        # Sorted, so concurrent block transactions lock counter rows in one order.
        for schema_name, table_name, variable in sorted(table_rows):
            parts.append(
                f"""
    UPDATE {DATA_TABLE_ROW_COUNTS}
    SET row_count = row_count + {variable},
        updated_at = clock_timestamp()
    WHERE schema_name = {sql_literal(schema_name)}
      AND table_name = {sql_literal(table_name)};
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Contador ausente para %.% durante insercao.',
            {sql_literal(schema_name)}, {sql_literal(table_name)};
    END IF;"""
            )
        return "".join(parts)

    # ------------------------------------------------------------- run lookup
    def _ensure_state(self):
        self._psql(
            "BEGIN;\nSET LOCAL client_min_messages = warning;\n"
            + state_ddl_sql()
            + "\nCOMMIT;\n",
            timing_name="Ensure resumable expansion state tables",
        )

    def _load_open_run(self):
        output = self._psql(
            f"""
SELECT json_build_object(
    'open_run', json_build_object(
        'run_id', run.run_id,
        'fingerprint', run.fingerprint,
        'status', run.status,
        'raw_hash_manifest', run.raw_hash_manifest,
        'expected_row_count', run.expected_row_count,
        'whole_source_rows', run.whole_source_rows,
        'blocks', (
            SELECT json_agg(json_build_object(
                'block_number', block.block_number,
                'raw_id_from', block.raw_id_from,
                'raw_id_to', block.raw_id_to,
                'source_rows', block.source_rows,
                'status', block.status,
                'destination_rows', block.destination_rows
            ) ORDER BY block.block_number)
            FROM {BLOCKS_TABLE} AS block
            WHERE block.run_id = run.run_id
        )
    )
)::text
FROM {RUNS_TABLE} AS run
WHERE run.status <> 'completed';
""",
            timing_name="Load resumable expansion run state",
        )
        runs = self._json_lines(output, "open_run")
        return runs[0]["open_run"] if runs else None

    # ------------------------------------------------------------ plan phase
    def _timing_table_sql(self) -> str:
        return """
CREATE TEMP TABLE IF NOT EXISTS pgdm_expansion_timings (
    sequence_number integer PRIMARY KEY,
    stage_key text NOT NULL,
    item_name text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    rows_affected bigint,
    destination_name text,
    statement_index integer
) ON COMMIT PRESERVE ROWS;
""".strip()

    @staticmethod
    def _timing_start(sequence: int, name: str) -> str:
        return (
            "INSERT INTO pg_temp.pgdm_expansion_timings VALUES "
            f"({sequence}, 'execute', {sql_literal(name)}, clock_timestamp(), "
            "NULL, NULL, NULL, NULL);"
        )

    @staticmethod
    def _timing_end(sequence: int) -> str:
        return (
            "UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() "
            f"WHERE sequence_number = {sequence};"
        )

    @staticmethod
    def _timing_result_sql() -> str:
        return """
SELECT json_build_object(
    'pgdm_expansion_timings', COALESCE(json_agg(json_build_object(
        'sequence', sequence_number,
        'stage', stage_key,
        'name', item_name,
        'duration_seconds', EXTRACT(EPOCH FROM (finished_at - started_at)),
        'rows', rows_affected,
        'destination', destination_name,
        'statement_index', statement_index
    ) ORDER BY sequence_number), '[]'::json)
)::text
FROM pg_temp.pgdm_expansion_timings;
DROP TABLE pg_temp.pgdm_expansion_timings;
""".strip()

    def _plan_run(self):
        whole_source_sql = []
        sequence = 10
        for destination in self.whole_source:
            for statement_index, statement in enumerate(destination["statements"], start=1):
                whole_source_sql.append(
                    self.service._build_managed_dml_sql(
                        statement,
                        destination["schema_name"],
                        destination["pure_table_name"],
                        "add",
                        timing_sequence=sequence,
                        timing_stage="execute",
                        timing_name=(
                            f"Insert into {destination['table_name']} "
                            f"(statement {statement_index}, whole source)"
                        ),
                        timing_destination=destination["table_name"],
                        timing_statement_index=statement_index,
                    )
                )
                sequence += 1
        destination_names = [item["table_name"] for item in self.prepared["destinations"]]
        destination_array = "ARRAY[" + ", ".join(sql_literal(v) for v in destination_names) + "]::text[]"
        scope_array = "ARRAY[" + self._scope_sql() + "]::text[]"
        # A multiple of the session count, so the last round of blocks does not
        # leave sessions idle while one large block finishes.
        block_count_sql = (
            f"(CEIL(CEIL((SELECT SUM(row_count) FROM {MANIFEST_TEMP_TABLE})::numeric "
            f"/ {int(self.block_rows)}) / {int(self.workers)}) * {int(self.workers)})::integer"
        )
        sql = "\n\n".join(
            [
                self._timing_table_sql(),
                "BEGIN ISOLATION LEVEL REPEATABLE READ;",
                "SET LOCAL client_min_messages = warning;",
                self._timing_start(2, "Acquire destination table locks"),
                self.lock_sql,
                self._timing_end(2),
                self._timing_start(3, "Scan source and build dependency manifest"),
                self.manifest_sql,
                self._timing_end(3),
                self._timing_start(4, "Register resumable expansion run"),
                f"""
INSERT INTO {RUNS_TABLE} (
    fingerprint, source_schema_name, source_table_name, raw_schemas,
    destination_names, raw_hash_manifest, expected_row_count, status
)
SELECT
    {sql_literal(self.fingerprint)},
    {sql_literal(self.prepared['source_schema_name'])},
    {sql_literal(self.prepared['source_table_name'])},
    {scope_array},
    {destination_array},
    COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'raw_schema', raw_schema, 'raw_hash', raw_hash, 'row_count', row_count
        ) ORDER BY raw_schema, raw_hash NULLS FIRST)
        FROM {MANIFEST_TEMP_TABLE}
    ), '[]'::jsonb),
    COALESCE((SELECT SUM(row_count) FROM {MANIFEST_TEMP_TABLE}), 0),
    'running';
""".strip(),
                self._timing_end(4),
                *whole_source_sql,
                self._timing_start(9000, "Plan raw_id blocks"),
                f"""
INSERT INTO {BLOCKS_TABLE} (run_id, block_number, raw_id_from, raw_id_to, source_rows, status)
SELECT
    currval(pg_get_serial_sequence({sql_literal(RUNS_TABLE)}, 'run_id')),
    block_number,
    MIN(raw_id),
    MAX(raw_id),
    COUNT(*),
    'pending'
FROM (
    SELECT {sql_ident('raw_id')} AS raw_id,
           NTILE({block_count_sql}) OVER (ORDER BY {sql_ident('raw_id')}) AS block_number
    FROM {self._source_sql()}
    WHERE {sql_ident('raw_schema')} IN ({self._scope_sql()})
) AS numbered
GROUP BY block_number;
""".strip(),
                f"""
UPDATE {RUNS_TABLE}
SET whole_source_rows = COALESCE((
        SELECT jsonb_object_agg(destination_name, rows_affected)
        FROM (
            SELECT destination_name, SUM(rows_affected) AS rows_affected
            FROM pg_temp.pgdm_expansion_timings
            WHERE destination_name IS NOT NULL
            GROUP BY destination_name
        ) AS totals
    ), '{{}}'::jsonb),
    updated_at = clock_timestamp()
WHERE run_id = currval(pg_get_serial_sequence({sql_literal(RUNS_TABLE)}, 'run_id'));
""".strip(),
                self._timing_end(9000),
                "COMMIT;",
                self._timing_result_sql(),
            ]
        )
        output = self._psql(sql, timing_name="Plan transaction envelope", envelope=True)
        manifests = self._json_lines(output, "raw_hash_manifest")
        timings = self._json_lines(output, "pgdm_expansion_timings")
        if not manifests:
            raise RuntimeError("The expansion plan did not return the source manifest.")
        if timings:
            self.server_timings.extend(timings[-1]["pgdm_expansion_timings"])
        return manifests[0]

    # ------------------------------------------------------------ block phase
    def _block_sql(self, run_id: int, block: dict) -> str:
        statements = []
        counters = []
        rows_json = []
        variable_index = 0
        for destination in self.per_block:
            variable = f"pgdm_rows_{variable_index}"
            variable_index += 1
            pieces = []
            for statement in destination["statements"]:
                pieces.append(
                    f"""
    {self._block_statement(statement, block['raw_id_from'], block['raw_id_to'])}
    ;
    GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    {variable} := {variable} + pgdm_affected_rows;"""
                )
            statements.append((variable, "".join(pieces)))
            counters.append((destination["schema_name"], destination["pure_table_name"], variable))
            rows_json.append(f"{sql_literal(destination['table_name'])}, {variable}")

        tag = "$pgdm_block$"
        while any(tag in body for _variable, body in statements):
            tag = tag[:-1] + "_x$"
        declarations = "".join(f"\n    {variable} bigint := 0;" for variable, _body in statements)
        body = "".join(text for _variable, text in statements)
        return f"""
SELECT current_setting('server_version_num')::integer >= 140000 AS pgdm_can_check_client \\gset
\\if :pgdm_can_check_client
SET client_connection_check_interval = '1s';
\\endif
-- READ COMMITTED: the shared row counters are updated by concurrent blocks and
-- must wait for each other instead of failing with a serialization error. The
-- source cannot change unnoticed: finalization re-validates its manifest.
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL client_min_messages = warning;
DO {tag}
DECLARE
    pgdm_affected_rows bigint;
    pgdm_block_status text;
    pgdm_block_started_at timestamptz := clock_timestamp();{declarations}
BEGIN
    SELECT status INTO pgdm_block_status
    FROM {BLOCKS_TABLE}
    WHERE run_id = {int(run_id)} AND block_number = {int(block['block_number'])}
    FOR UPDATE;
    IF pgdm_block_status IS NULL THEN
        RAISE EXCEPTION 'Expansion block % of run % does not exist.',
            {int(block['block_number'])}, {int(run_id)};
    END IF;
    IF pgdm_block_status = 'done' THEN
        RAISE NOTICE 'pgdm block already done';
        RETURN;
    END IF;
{body}
{self._counter_sql(counters)}

    UPDATE {BLOCKS_TABLE}
    SET status = 'done',
        destination_rows = jsonb_build_object({', '.join(rows_json)}),
        started_at = pgdm_block_started_at,
        finished_at = clock_timestamp()
    WHERE run_id = {int(run_id)} AND block_number = {int(block['block_number'])};
END
{tag};
COMMIT;
SELECT json_build_object(
    'pgdm_block', block_number,
    'status', status,
    'started_at', EXTRACT(EPOCH FROM started_at),
    'finished_at', EXTRACT(EPOCH FROM finished_at),
    'destination_rows', destination_rows
)::text
FROM {BLOCKS_TABLE}
WHERE run_id = {int(run_id)} AND block_number = {int(block['block_number'])};
""".strip()

    def _run_blocks(self, run_id: int, blocks):
        pending = [block for block in blocks if block["status"] != "done"]
        if not pending:
            return []
        results = []
        lock = threading.Lock()
        done_count = [len(blocks) - len(pending)]

        def run_block(block):
            self.service._raise_if_cancelled(self.cancel_event)
            output = self._psql(self._block_sql(run_id, block))
            lines = self._json_lines(output, "pgdm_block")
            if not lines or lines[-1].get("status") != "done":
                raise RuntimeError(
                    f"Expansion block {block['block_number']} did not confirm completion."
                )
            with lock:
                done_count[0] += 1
                self._progress(
                    10 + 85 * done_count[0] / len(blocks),
                    f"Committed block {done_count[0]}/{len(blocks)}.",
                )
            return lines[-1]

        with ThreadPoolExecutor(max_workers=min(self.workers, len(pending))) as executor:
            futures = [executor.submit(run_block, block) for block in pending]
            first_error = None
            for future in futures:
                try:
                    results.append(future.result())
                except Exception as exc:  # keep draining so no session is left behind
                    if first_error is None:
                        first_error = exc
                        # Queued blocks are dropped; running blocks either commit
                        # (and are recorded as done) or roll back as a whole.
                        for other in futures:
                            other.cancel()
            if first_error is not None:
                raise first_error
        return results

    # --------------------------------------------------------- finalize phase
    def _finalize(self, run_id: int, stored_manifest) -> dict:
        sql = "\n\n".join(
            [
                self._timing_table_sql(),
                "BEGIN ISOLATION LEVEL REPEATABLE READ;",
                "SET LOCAL client_min_messages = warning;",
                self._timing_start(9100, "Verify blocks and re-validate source manifest"),
                self.manifest_sql,
                f"""
DO $pgdm_finalize$
DECLARE
    pending_blocks bigint;
    stored_manifest jsonb;
    current_manifest jsonb;
BEGIN
    SELECT COUNT(*) INTO pending_blocks
    FROM {BLOCKS_TABLE}
    WHERE run_id = {int(run_id)} AND status <> 'done';
    IF pending_blocks > 0 THEN
        RAISE EXCEPTION 'Expansion run % still has % pending block(s).', {int(run_id)}, pending_blocks;
    END IF;

    SELECT raw_hash_manifest INTO stored_manifest FROM {RUNS_TABLE} WHERE run_id = {int(run_id)} FOR UPDATE;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'raw_schema', raw_schema, 'raw_hash', raw_hash, 'row_count', row_count
    ) ORDER BY raw_schema, raw_hash NULLS FIRST), '[]'::jsonb)
    INTO current_manifest
    FROM {MANIFEST_TEMP_TABLE};
    IF current_manifest IS DISTINCT FROM stored_manifest THEN
        RAISE EXCEPTION 'The source changed while expansion run % was in progress.', {int(run_id)};
    END IF;

    UPDATE {RUNS_TABLE}
    SET status = 'blocks_done', updated_at = clock_timestamp()
    WHERE run_id = {int(run_id)} AND status IN ('running', 'blocks_done');
END
$pgdm_finalize$;
""".strip(),
                self._timing_end(9100),
                "COMMIT;",
                self._timing_result_sql(),
            ]
        )
        output = self._psql(sql, timing_name="Finalization transaction envelope", envelope=True)
        manifests = self._json_lines(output, "raw_hash_manifest")
        timings = self._json_lines(output, "pgdm_expansion_timings")
        if timings:
            self.server_timings.extend(timings[-1]["pgdm_expansion_timings"])
        if not manifests:
            raise RuntimeError("The expansion finalization did not return the source manifest.")
        return manifests[0]

    def _registered_versions(self, run_id: int):
        from services.postgres_service import (
            CONTROL_DB,
            CONTROL_TABLE_VERSION_DEPENDENCIES,
            CONTROL_TABLE_VERSIONS,
        )

        output = self._psql(
            f"""
SELECT json_build_object(
    'table_name', version.schema_name || '.' || version.table_name,
    'version_code', version.version_code
)::text
FROM {CONTROL_TABLE_VERSION_DEPENDENCIES} AS dependency
JOIN {CONTROL_TABLE_VERSIONS} AS version ON version.id = dependency.table_version_id
WHERE version.database_name = {sql_literal(self.database_name)}
  AND dependency.dependency_payload ->> 'expansion_run_id' = {sql_literal(str(run_id))}
  AND dependency.dependency_payload ->> 'expansion_run_fingerprint' = {sql_literal(self.fingerprint)}
ORDER BY version.schema_name, version.table_name;
""",
            database_name=CONTROL_DB,
            timing_name="Check versions already registered for the run",
        )
        return self._json_lines(output, "version_code")

    # ------------------------------------------------------------------- run
    def run(self) -> dict:
        self._ensure_state()
        open_run = self._load_open_run()
        resumed = open_run is not None
        if open_run and open_run["fingerprint"] != self.fingerprint:
            raise RuntimeError(
                f"An interrupted expansion (run {open_run['run_id']}) with a different "
                "source, scope or recipes is still open in this database. Resume it by "
                "running Expand again with the same source, scope and recipes before "
                "starting a different expansion."
            )

        parallel_started = None
        if open_run is None:
            self._progress(5, "Planning resumable blocks and expanding whole-source destinations...")
            self._plan_run()
            open_run = self._load_open_run()
            if open_run is None:
                raise RuntimeError("The expansion run was not registered.")
        run_id = int(open_run["run_id"])

        if open_run["status"] == "running":
            blocks = open_run.get("blocks") or []
            pending = sum(1 for block in blocks if block["status"] != "done")
            self._progress(
                10,
                f"Run {run_id}: {pending} of {len(blocks)} block(s) to expand "
                f"with up to {self.workers} parallel session(s)...",
            )
            parallel_started = time.perf_counter()
            block_results = self._run_blocks(run_id, blocks)
            parallel_seconds = time.perf_counter() - parallel_started
            if block_results:
                started = min(float(item["started_at"]) for item in block_results)
                finished = max(float(item["finished_at"]) for item in block_results)
                self.server_timings.append(
                    {
                        "sequence": 5000,
                        "stage": "execute",
                        "name": (
                            f"Expand {len(block_results)} raw_id block(s) in "
                            f"{min(self.workers, len(block_results))} parallel session(s) "
                            "(first block start to last block commit)"
                        ),
                        "duration_seconds": max(finished - started, 0.0),
                        "rows": None,
                        "destination": None,
                        "statement_index": None,
                    }
                )
                for item in block_results:
                    self.server_timings.append(
                        {
                            "diagnostic": True,
                            "name": f"Block {item['pgdm_block']} transaction",
                            "duration_seconds": float(item["finished_at"]) - float(item["started_at"]),
                            "details": json.dumps(item.get("destination_rows") or {}, sort_keys=True),
                        }
                    )
                self.server_timings.append(
                    {
                        "diagnostic": True,
                        "name": "Parallel block phase as seen by the client",
                        "duration_seconds": parallel_seconds,
                        "details": "Includes SSH round trips; excluded from totals.",
                    }
                )
            manifest = self._finalize(run_id, open_run["raw_hash_manifest"])
            open_run = self._load_open_run()
        else:
            manifest = self._finalize(run_id, open_run["raw_hash_manifest"])

        already_registered = self._registered_versions(run_id) if resumed else []
        manifest["expansion_run_id"] = run_id
        manifest["expansion_run_fingerprint"] = self.fingerprint
        destination_rows = dict(open_run.get("whole_source_rows") or {})
        for block in open_run.get("blocks") or []:
            for table_name, rows in (block.get("destination_rows") or {}).items():
                destination_rows[table_name] = destination_rows.get(table_name, 0) + int(rows)
        return {
            "run_id": run_id,
            "resumed": resumed,
            "manifest": manifest,
            "server_timings": self.server_timings,
            "destination_rows": destination_rows,
            "block_count": len(open_run.get("blocks") or []),
            "already_registered_versions": already_registered,
        }


def complete_run_sql(run_id: int, fingerprint: str) -> str:
    return f"""
UPDATE {RUNS_TABLE}
SET status = 'completed',
    completed_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE run_id = {int(run_id)}
  AND fingerprint = {sql_literal(fingerprint)}
  AND status = 'blocks_done';
""".strip()


def automatic_workers(service, cancel_event=None) -> int:
    try:
        output = service.run_remote_command("nproc", cancel_event=cancel_event)
        cpus = int(str(output).strip().splitlines()[-1])
    except Exception:
        cpus = 1
    return max(1, min(cpus, MAX_DEFAULT_WORKERS))

