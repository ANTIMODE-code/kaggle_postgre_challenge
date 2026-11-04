"""Baseline headless dell'operazione Expand (Fase 0).

Riproduce il flusso di main.py::_execute_cross_table_expansion_thread senza GUI:
connessione SSH -> ensure_admin_schema -> execute_cross_table_expansion ->
register_cross_table_expansion_versions -> list_tables, con lo stesso timing_report
dell'applicazione e lo stesso testo dell'Expansion Performance Report.

Per ogni dimensione richiesta:
  1. rigenera kaggle_challenge con generate_database.py ufficiale (--replace);
  2. esegue Expand con le sei ricette ufficiali in ordine;
  3. salva report .txt/.json, subtotali per fase, RAM (PSS) di picco dei backend
     PostgreSQL, byte temporanei e checksum canonici delle tabelle di destinazione.

Uso (un solo comando):
  python run_baseline.py --repo /path/kaggle_postgre_challenge --rows 100000 1000000 10000000

Credenziali solo da variabili d'ambiente (mai nel repository):
  PDM_SSH_HOST, PDM_SSH_PORT, PDM_SSH_USER, PDM_SSH_PASS,
  PDM_PG_PORT, PDM_PG_USER, PG_KAGGLE_CHALLENGE_PASS
"""

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

DATABASE = "kaggle_challenge"
RECIPES = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)
STAGES = ("prepare", "preflight", "execute", "version", "refresh")
HERE = Path(__file__).resolve().parent


def env(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        sys.exit(f"Variabile d'ambiente mancante: {name}")
    return value


class PostgresMemorySampler:
    """Campiona la somma della PSS di tutti i processi postgres (memoria condivisa
    ripartita, quindi niente doppi conteggi). Funziona solo se PostgreSQL gira
    sulla stessa macchina del benchmark; altrimenti resta a None."""

    def __init__(self, interval=0.25):
        self.interval = interval
        self.peak_kb = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _sample(self):
        total = 0
        found = False
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                if Path(f"/proc/{pid}/comm").read_text().strip() != "postgres":
                    continue
                for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
                    if line.startswith("Pss:"):
                        total += int(line.split()[1])
                        found = True
                        break
            except (OSError, ValueError):
                continue
        return total if found else None

    def _run(self):
        while not self._stop.is_set():
            value = self._sample()
            if value is not None:
                self.peak_kb = max(self.peak_kb or 0, value)
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._stop.set()
        self._thread.join()


def psql(sql, database=DATABASE):
    result = subprocess.run(
        [
            "psql", "-h", env("PDM_PG_HOST", "localhost"), "-p", env("PDM_PG_PORT", "5432"),
            "-U", env("PDM_PG_USER"), "-d", database, "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
            "-c", sql,
        ],
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "PGPASSWORD": env("PG_KAGGLE_CHALLENGE_PASS")},
    )
    return result.stdout.strip()


def generate(repo, rows, python):
    started = time.perf_counter()
    subprocess.run(
        [
            python, "generate_database.py", "--rows", str(rows), "--replace",
            "--host", env("PDM_PG_HOST", "localhost"), "--postgres_port", env("PDM_PG_PORT", "5432"),
            "--user", env("PDM_PG_USER"), "--admin_database", "postgres",
        ],
        cwd=repo,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return time.perf_counter() - started


def run_expand(repo, report_dialog_cls, service_cls):
    service = service_cls()
    service.connect(
        env("PDM_SSH_HOST", "127.0.0.1"),
        int(env("PDM_SSH_PORT", "22")),
        env("PDM_SSH_USER"),
        env("PDM_SSH_PASS"),
        int(env("PDM_PG_PORT", "5432")),
        env("PDM_PG_USER"),
        env("PG_KAGGLE_CHALLENGE_PASS"),
    )
    destinations = [
        {
            "table_name": table_name,
            "version_title": "Synthetic expansion",
            "sql": (repo / "SQL files" / file_name).read_text(encoding="utf-8"),
        }
        for file_name, table_name in RECIPES
    ]
    report = service.create_cross_table_expansion_timing_report(
        DATABASE, "public.raw_data", ["group001"], [d["table_name"] for d in destinations]
    )
    wall_started = time.perf_counter()
    try:
        # Stesso ordine di main.py::_execute_cross_table_expansion_thread.
        with service.expansion_timing_scope(report, "prepare", "Ensure control database metadata"):
            service.ensure_admin_schema()
        result = service.execute_cross_table_expansion(
            database_name=DATABASE,
            source_full_table_name="public.raw_data",
            raw_schemas=["group001"],
            destinations=destinations,
            timing_report=report,
        )
        registered = list(
            (result.get("expansion_run") or {}).get("already_registered_versions") or []
        ) or service.register_cross_table_expansion_versions(
            expansion_result=result,
            requested_by="baseline",
            workstation_name=platform.node(),
            timing_report=report,
        )
        # Solo nella versione ottimizzata (expansion_run): chiude la run ripresa.
        if hasattr(service, "complete_cross_table_expansion_run"):
            service.complete_cross_table_expansion_run(result, timing_report=report)
        # Invalidazione cache SQLite e ricostruzione dei pulsanti sono solo GUI:
        # nel run headless restano fuori (vedi README).
        with service.expansion_timing_scope(report, "refresh", "Reload database table catalog"):
            service.list_tables(DATABASE)
        report["completed_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    finally:
        service.close()
    wall = time.perf_counter() - wall_started
    return report, registered, wall, report_dialog_cls.build_report_text(report)


def canonical_checksums():
    sql = (HERE / "canonical_checksums.sql").read_text(encoding="utf-8")
    return json.loads(psql(sql))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--rows", nargs="+", type=int, default=[100_000, 1_000_000, 10_000_000])
    parser.add_argument("--label", default="baseline")
    parser.add_argument("--out", type=Path, default=HERE / "results")
    parser.add_argument("--skip-generate", action="store_true",
                        help="usa il database esistente (solo se le destinazioni sono vuote)")
    args = parser.parse_args()

    repo = args.repo.resolve()
    sys.path.insert(0, str(repo))
    from services.postgres_service import PostgresAdminService
    from ui.dialogs import ExpansionTimingReportDialog

    commit = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                            capture_output=True, text=True).stdout.strip()
    args.out.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    for rows in args.rows:
        print(f"\n=== {args.label}: {rows:,} righe ===", flush=True)
        generation_seconds = None
        if not args.skip_generate:
            generation_seconds = generate(repo, rows, sys.executable)
            print(f"Generazione: {generation_seconds:.1f} s", flush=True)
        try:
            # Riduce il rumore del checkpoint post-generazione; serve un ruolo con
            # pg_checkpoint (PG15+) o superuser, altrimenti si prosegue senza.
            psql("CHECKPOINT")
        except subprocess.CalledProcessError:
            print("CHECKPOINT non permesso al ruolo: proseguo", flush=True)
        temp_before = int(psql("SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()"))
        with PostgresMemorySampler() as sampler:
            report, registered, wall, text = run_expand(repo, ExpansionTimingReportDialog, PostgresAdminService)
        temp_bytes = int(psql("SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()")) - temp_before
        checksums = canonical_checksums()
        db_size = int(psql("SELECT pg_database_size(current_database())"))

        stage_totals = {
            stage: sum(i["seconds"] for i in report["items"] if i["stage"] == stage and i["include_in_total"])
            for stage in STAGES
        }
        total = ExpansionTimingReportDialog.measured_total_seconds(report)
        base = args.out / f"{args.label}_{rows}"
        base.with_suffix(".txt").write_text(text, encoding="utf-8")
        base.with_suffix(".json").write_text(json.dumps({
            "label": args.label,
            "rows": rows,
            "repo_commit": commit,
            "postgres_version": psql("SHOW server_version"),
            "python_version": platform.python_version(),
            "host": {"cpus": os.cpu_count(), "platform": platform.platform()},
            "generation_seconds": generation_seconds,
            "client_wall_seconds": wall,
            "measured_total_seconds": total,
            "stage_seconds": stage_totals,
            "peak_postgres_pss_kb": sampler.peak_kb,
            "temp_bytes": temp_bytes,
            "database_size_bytes": db_size,
            "registered_versions": registered,
            "checksums": checksums,
            "report": report,
        }, indent=2, default=str), encoding="utf-8")
        summary_rows.append({
            "label": args.label,
            "rows": rows,
            **{f"{s}_s": round(v, 3) for s, v in stage_totals.items()},
            "measured_total_s": round(total, 3),
            "client_wall_s": round(wall, 3),
            "peak_pg_pss_gb": round(sampler.peak_kb / 1024 ** 2, 3) if sampler.peak_kb else None,
            "temp_mb": round(temp_bytes / 1024 ** 2, 1),
            "db_size_gb": round(db_size / 1024 ** 3, 2),
        })
        print(text.split("MEASURED PROCESSING TOTAL")[0][-1200:])
        print(f"MEASURED PROCESSING TOTAL: {total:.3f} s | wall client {wall:.1f} s", flush=True)

    summary = args.out / f"{args.label}_summary.csv"
    write_header = not summary.exists()
    with summary.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        if write_header:
            writer.writeheader()
        writer.writerows(summary_rows)
    print(f"\nRiepilogo: {summary}")


if __name__ == "__main__":
    main()
