# Parallel, resumable Expand

The cross-table **Expand** now runs the official recipes in parallel PostgreSQL
sessions and can be resumed after an interruption. The recipe text, the
relational output, the validations, the row counters and the version history
are unchanged; only the way the work is scheduled inside PostgreSQL changes.

## What changed

The original Expand sends one transaction to `psql`: lock the destinations,
scan the source manifest, run the six `INSERT ... SELECT` recipes, commit.
`INSERT ... SELECT` never uses parallel query in PostgreSQL, so the whole
expansion runs on one CPU core, and an interruption loses all of it.

The new Expand (`services/parallel_expansion.py`) splits it into:

1. **Plan transaction** (REPEATABLE READ, destinations locked in
   `SHARE ROW EXCLUSIVE` exactly as before). It captures the source manifest
   (`raw_schema`, `raw_hash`, row count), registers a run in
   `pgdm_expansion.runs`, executes the recipes that must see the whole source
   (any recipe with `DISTINCT`, aggregates, window functions, set operations,
   `LIMIT`, or more than one read of `{{source}}`; here `table3` and `table4`),
   and splits the source rows of the selected scope into `raw_id` blocks of
   equal size with `NTILE` (a multiple of the session count, at most
   `PDM_EXPANSION_BLOCK_ROWS` rows each, so every round keeps all sessions busy). All of this commits together.
2. **Block transactions**, executed by up to `PDM_EXPANSION_WORKERS` sessions at
   once. Each block runs the remaining recipes (`table1`, `table5`, `table6`,
   `table2`) with `{{source}}` narrowed to its range: the protected subquery
   `SELECT * FROM source WHERE raw_schema IN (...)` gains
   `AND raw_id BETWEEN a AND b`, nothing else. In the same transaction it adds
   its inserted rows to `public.pgdm_table_row_counts` and marks the block
   `done`. The block row is locked `FOR UPDATE` first, and a block that is
   already `done` does nothing, so no block can be applied twice.
   A recipe that joins rows written earlier in the same block (here `table5`,
   `table6` and `table2` look up `table1` by its natural key) runs with
   `enable_hashjoin` and `enable_mergejoin` off, set with `set_config(..., true)`
   for that statement only and restored afterwards. Without it, once
   autovacuum analyzes the growing destination tables mid-run, the planner
   switches to hash joins that read the whole of `table1` or hash wide JSON rows
   for every block, and spill to temporary files (3M rows, 24 blocks: 4.4 GB of
   temporary files without the setting, 63 MB with it; 10M rows on PostgreSQL
   18: 18.3 GB before the fix). The index lookups it forces use the existing
   unique indexes, so no index is added.
3. **Finalization transaction**: every block must be `done` and the source
   manifest is computed again and must equal the one stored at plan time. Any
   change to the source during the run fails the expansion.
4. **Version registration**: the existing atomic version batch in the control
   database, unchanged, with `expansion_run_id` and
   `expansion_run_fingerprint` added to each dependency payload. The run is
   then marked `completed`.

Recipes run in the order given. A recipe that needs the whole source must come
before every per-row recipe, otherwise Expand refuses to parallelize it.

## Resuming

Run **Expand** again with the same source, scope and recipes. The run is
identified by a SHA-256 fingerprint of source, scope and prepared recipes:

| Interrupted during | State left | What the next Expand does |
|---|---|---|
| Preflight, or plan transaction aborted on the server | nothing (rolled back) | starts a new run |
| Block phase | committed blocks are `done`, the rest `pending` | runs only the pending blocks |
| Finalization | all blocks `done` | re-validates and finalizes |
| Between version batch and run completion | versions registered | finds the versions by run id, does not register them again, completes the run |

Closing or killing the Windows client does not necessarily stop the work:
each `psql` on the Linux server has already received its whole batch and may
finish it. That is safe: a batch that finishes commits a complete plan or a
complete block, a batch that is aborted on the server rolls back, and the next
Expand picks up whatever is still pending.

Only one run can be open per database (unique partial index). If an open run
has a different fingerprint, Expand stops and asks to resume that run first,
so partial data from two different expansions can never mix. No interruption
can leave a block half applied, because each block is one transaction. On
PostgreSQL 14+ the block sessions also set `client_connection_check_interval`,
so a backend whose `psql` disappeared aborts within a second instead of
holding its block lock.

Tested interruptions (1M rows, `resume_test.py`): client SIGKILL during the
plan and during the blocks, process exit right after the plan commit and right
after the version batch, user cancel during the blocks, and
`pg_terminate_backend` of the plan session and of all block sessions. After
each, a normal Expand finished the run with output checksums identical to the
uninterrupted single-transaction Expand, row counters equal to `COUNT(*)`,
exactly one version per destination, and no open run.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PDM_EXPANSION_WORKERS` | CPUs of the Linux server (`nproc`), max 8 | parallel PostgreSQL sessions |
| `PDM_EXPANSION_BLOCK_ROWS` | `500000` | maximum source rows per block (resume granularity) |
| `PDM_EXPANSION_BLOCK_JOIN_HINT` | `on` | `off` lets the planner choose join methods freely inside blocks (use it only if a custom recipe joins a block table without a usable index) |
| `PDM_EXPANSION_MODE` | parallel | `single_transaction` runs the original one-transaction Expand |

Objects added to the data database: schema `pgdm_expansion` with tables
`runs` and `blocks` (created on first use, hidden from the table list). No
extensions, no server settings and no new functions are required.

## Performance report

The measured total keeps the original model. Every bookkeeping command is
timed by the server; the plan and finalization steps report their own
`clock_timestamp()` sub-steps. The block phase counts **once**, from the first
block start to the last block commit; each block transaction and the client
view of the phase are listed as diagnostic lines, excluded from the total, so
parallel work is never summed.

## Known trade-offs

- Isolation: the original Expand held its destination locks until the single
  commit, so other sessions could not write to the destinations during the
  whole expansion and saw either nothing or everything. Now the plan
  transaction holds those locks, and each block is visible as soon as it
  commits. Foreign keys, unique constraints and the counters remain enforced
  for every block; an interrupted run is visible as partial until resumed.
- Block transactions use READ COMMITTED so concurrent counter updates wait for
  each other instead of failing. The source cannot change unnoticed: its
  manifest is re-validated at finalization.
- Correctness of the split assumes per-row recipes only read rows derived from
  the same source row (true for the official recipes: `table5`, `table6` and
  `table2` join `table1` on `raw_id`). Use `PDM_EXPANSION_MODE=single_transaction`
  for recipes that do not satisfy this.
