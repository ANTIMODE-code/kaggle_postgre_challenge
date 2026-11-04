"""Test di interruzione e ripresa dell'Expand parallelo.

Per ogni scenario: rigenera kaggle_challenge (generate_database.py ufficiale),
avvia l'Expand in un processo separato, lo interrompe in un punto preciso, poi
rilancia l'Expand normale (che riprende) e verifica:
  - checksum canonici identici alla baseline (nessuna riga persa o duplicata);
  - contatori pgdm_table_row_counts uguali a COUNT(*) reale;
  - esattamente una versione registrata per destinazione per la run;
  - nessuna run aperta e tutti i blocchi completati.

Uso:
  python resume_test.py --repo /path/repo --rows 1000000 \
      --baseline ../fase0/results/baseline_1000000.json
Stesse variabili d'ambiente di fase0/run_baseline.py.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run_baseline as rb  # noqa: E402

SCENARIOS = {
    "kill_during_plan": "SIGKILL del client mentre la transazione di piano è in corso (il psql lato server, che ha già ricevuto lo script, può completarla)",
    "terminate_plan_backend": "pg_terminate_backend della sessione PostgreSQL durante la transazione di piano (rollback lato server)",
    "terminate_block_backends": "pg_terminate_backend di tutte le sessioni dei blocchi con almeno 2 blocchi confermati",
    "exit_after_plan": "uscita del processo subito dopo il commit del piano, nessun blocco eseguito",
    "kill_during_blocks": "SIGKILL del client con almeno 2 blocchi già confermati e altri in corso",
    "cancel_during_blocks": "Annulla dell'utente (cancel_event) durante i blocchi",
    "exit_after_versions": "uscita dopo la registrazione delle versioni, prima di chiudere la run",
}


def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def child(repo: Path, scenario: str):
    """Esegue un Expand con un punto di interruzione deterministico."""
    sys.path.insert(0, str(repo))
    from services import parallel_expansion
    from services.postgres_service import OperationCancelledError, PostgresAdminService
    from ui.dialogs import ExpansionTimingReportDialog

    if scenario == "exit_after_plan":
        original = parallel_expansion.ParallelExpansionRunner._plan_run

        def plan_then_die(self):
            result = original(self)
            log("piano confermato: uscita immediata")
            os._exit(137)

        parallel_expansion.ParallelExpansionRunner._plan_run = plan_then_die

    if scenario == "exit_after_versions":
        original_register = PostgresAdminService.register_cross_table_expansion_versions

        def register_then_die(self, *args, **kwargs):
            result = original_register(self, *args, **kwargs)
            log(f"versioni registrate ({len(result)}): uscita immediata")
            os._exit(137)

        PostgresAdminService.register_cross_table_expansion_versions = register_then_die

    if scenario == "cancel_during_blocks":
        cancel_event = threading.Event()
        original_service_init = PostgresAdminService.execute_cross_table_expansion

        def execute_with_cancel(self, *args, **kwargs):
            kwargs["cancel_event"] = cancel_event

            def watcher():
                while not cancel_event.is_set():
                    if done_blocks() >= 2:
                        log("2 blocchi confermati: Annulla")
                        cancel_event.set()
                    time.sleep(0.2)

            threading.Thread(target=watcher, daemon=True).start()
            return original_service_init(self, *args, **kwargs)

        PostgresAdminService.execute_cross_table_expansion = execute_with_cancel

    try:
        rb.run_expand(repo, ExpansionTimingReportDialog, PostgresAdminService)
    except OperationCancelledError as exc:
        log(f"annullato: {exc}")
        sys.exit(3)
    log("Expand terminato senza interruzione")


def done_blocks() -> int:
    try:
        return int(rb.psql(
            "SELECT COUNT(*) FROM pgdm_expansion.blocks b JOIN pgdm_expansion.runs r USING (run_id) "
            "WHERE r.status <> 'completed' AND b.status = 'done'"
        ) or 0)
    except subprocess.CalledProcessError:
        return 0


def plan_in_progress() -> bool:
    return rb.psql(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() "
        "AND state = 'active' AND query LIKE '%INSERT INTO public.table4%' AND pid <> pg_backend_pid()"
    ) not in ("", "0")


def terminate(pattern: str) -> int:
    return int(rb.psql(
        "SELECT COUNT(pg_terminate_backend(pid)) FROM pg_stat_activity "
        "WHERE datname = current_database() AND state <> 'idle' "
        f"AND query LIKE '{pattern}' AND pid <> pg_backend_pid()"
    ) or 0)


def run_child(repo: Path, scenario: str):
    process = subprocess.Popen([sys.executable, __file__, "--child", scenario, "--repo", str(repo)])
    if scenario in ("terminate_plan_backend", "terminate_block_backends"):
        while process.poll() is None:
            if scenario == "terminate_plan_backend" and plan_in_progress():
                time.sleep(0.5)
                log(f"piano in corso: terminate di {terminate('%INSERT INTO public.table4%')} backend")
                break
            if scenario == "terminate_block_backends" and done_blocks() >= 2:
                log(f"{done_blocks()} blocchi confermati: terminate di {terminate('%pgdm_block_status%')} backend")
                break
            time.sleep(0.1)
    if scenario in ("kill_during_plan", "kill_during_blocks"):
        while process.poll() is None:
            if scenario == "kill_during_plan" and plan_in_progress():
                time.sleep(0.5)
                log("transazione di piano in corso: SIGKILL")
                process.send_signal(signal.SIGKILL)
                break
            if scenario == "kill_during_blocks" and done_blocks() >= 2:
                log(f"{done_blocks()} blocchi confermati: SIGKILL")
                process.send_signal(signal.SIGKILL)
                break
            time.sleep(0.1)
    process.wait()
    return process.returncode


def verify(baseline: dict) -> dict:
    checks = {}
    checks["checksums_equal_baseline"] = rb.canonical_checksums() == baseline["checksums"]
    counters = json.loads(rb.psql("""
SELECT json_object_agg(c.table_name, json_build_object('counter', c.row_count, 'actual', a.n))::text
FROM public.pgdm_table_row_counts c
JOIN LATERAL (SELECT (xpath('/row/n/text()', query_to_xml(format('SELECT COUNT(*) AS n FROM %I.%I', c.schema_name, c.table_name), false, true, '')))[1]::text::bigint AS n) a ON true
WHERE c.schema_name = 'public' AND c.table_name ~ '^table[1-6]$'
"""))
    checks["counters"] = counters
    checks["counters_match"] = all(v["counter"] == v["actual"] for v in counters.values())
    runs = json.loads(rb.psql(
        "SELECT json_agg(json_build_object('run_id', run_id, 'status', status, "
        "'blocks', (SELECT COUNT(*) FROM pgdm_expansion.blocks b WHERE b.run_id = r.run_id), "
        "'blocks_done', (SELECT COUNT(*) FROM pgdm_expansion.blocks b WHERE b.run_id = r.run_id AND b.status = 'done')"
        ") ORDER BY run_id)::text FROM pgdm_expansion.runs r"
    ) or "[]")
    checks["runs"] = runs
    checks["single_completed_run"] = (
        len(runs) == 1 and runs[0]["status"] == "completed" and runs[0]["blocks"] == runs[0]["blocks_done"]
    )
    versions = json.loads(rb.psql(
        """
SELECT json_agg(json_build_object(
    'table', v.table_name, 'version', v.version_code,
    'run_id', d.dependency_payload ->> 'expansion_run_id') ORDER BY v.table_name, v.version_code)::text
FROM app_control.table_versions v
JOIN app_control.table_version_dependencies d ON d.table_version_id = v.id
WHERE v.database_name = 'kaggle_challenge'
""",
        database="postgres_data_manager",
    ) or "[]")
    checks["versions"] = versions
    per_table = {}
    for item in versions:
        per_table[item["table"]] = per_table.get(item["table"], 0) + 1
    checks["one_version_per_destination"] = (
        sorted(per_table) == [f"table{i}" for i in range(1, 7)] and set(per_table.values()) == {1}
        and len({item["run_id"] for item in versions}) == 1
    )
    checks["passed"] = all(
        checks[key] for key in ("checksums_equal_baseline", "counters_match", "single_completed_run", "one_version_per_destination")
    )
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--scenarios", nargs="+", default=list(SCENARIOS))
    parser.add_argument("--out", type=Path, default=HERE / "results")
    parser.add_argument("--child")
    args = parser.parse_args()
    repo = args.repo.resolve()
    if args.child:
        child(repo, args.child)
        return

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    if baseline["rows"] != args.rows:
        sys.exit("La baseline deve avere lo stesso numero di righe.")
    args.out.mkdir(parents=True, exist_ok=True)
    results = {}
    for scenario in args.scenarios:
        log(f"=== {scenario}: {SCENARIOS[scenario]} ===")
        rb.generate(repo, args.rows, sys.executable)
        code = run_child(repo, scenario)
        log(f"processo interrotto, exit code {code}; blocchi confermati: {done_blocks()}")
        log("ripresa: Expand normale")
        started = time.perf_counter()
        report, registered, wall, _text = rb.run_expand(repo, *_classes(repo))
        checks = verify(baseline)
        checks["interrupted_exit_code"] = code
        checks["resume_wall_seconds"] = wall
        checks["resume_report_run"] = report.get("expansion_run")
        results[scenario] = checks
        log(f"esito: {'OK' if checks['passed'] else 'FALLITO'} | {json.dumps({k: checks[k] for k in ('checksums_equal_baseline', 'counters_match', 'single_completed_run', 'one_version_per_destination')})}")
    (args.out / f"resume_{args.rows}.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    sys.exit(0 if all(r["passed"] for r in results.values()) else 1)


def _classes(repo):
    sys.path.insert(0, str(repo))
    from services.postgres_service import PostgresAdminService
    from ui.dialogs import ExpansionTimingReportDialog
    return ExpansionTimingReportDialog, PostgresAdminService


if __name__ == "__main__":
    main()
