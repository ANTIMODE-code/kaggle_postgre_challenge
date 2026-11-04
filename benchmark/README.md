# Benchmark and resume tests

Local, unofficial tools used to measure Expand and to test interruption and
resume. Comments and console messages are in Italian.

- `run_baseline.py`: regenerates `kaggle_challenge` with the official
  `generate_database.py` for each size, runs Expand headless with the same
  code path and Expansion Performance Report as the GUI, and saves the report,
  per-stage subtotals, peak PostgreSQL PSS, temporary bytes, and canonical
  checksums of the six destinations.
- `canonical_checksums.sql`: per-table row count and checksum with identity
  keys replaced by natural keys (raw_id, table3 content), so runs that assign
  identity values in a different order remain comparable.
- `resume_test.py`: interrupts Expand at seven different points (client
  SIGKILL, process exit, user cancel, `pg_terminate_backend`), runs Expand again,
  and checks checksums against a baseline, counters against `COUNT(*)`, one
  version per destination, and no open run.

```bash
export PDM_SSH_HOST=<linux_host> PDM_SSH_PORT=22 PDM_SSH_USER=<ssh_user> PDM_SSH_PASS=<ssh_password>
export PDM_PG_PORT=5432 PDM_PG_USER=<postgres_user> PG_KAGGLE_CHALLENGE_PASS=<postgres_password>
# reference output with the original single-transaction Expand
PDM_EXPANSION_MODE=single_transaction python benchmark/run_baseline.py --repo . --rows 1000000 --label baseline
python benchmark/run_baseline.py --repo . --rows 1000000 --label parallel
python benchmark/resume_test.py --repo . --rows 1000000 --baseline benchmark/results/baseline_1000000.json
```

Requires Python with tkinter (the report text comes from the GUI dialog class)
and `psql` on the machine running the scripts. PSS sampling only works when
PostgreSQL runs on the same machine.
