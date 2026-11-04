import json
import unittest
from pathlib import Path

from services import parallel_expansion
from services.postgres_service import PostgresAdminService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECIPE_DIR = PROJECT_ROOT / "SQL files"
RECIPE_DESTINATIONS = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)


def prepare_official_recipes():
    return PostgresAdminService._prepare_cross_table_expansion(
        "public.raw_data",
        ["group001"],
        [
            {
                "table_name": table_name,
                "version_title": "Synthetic expansion",
                "sql": (RECIPE_DIR / file_name).read_text(encoding="utf-8"),
            }
            for file_name, table_name in RECIPE_DESTINATIONS
        ],
    )


def build_runner(prepared, workers=4):
    return parallel_expansion.ParallelExpansionRunner(
        PostgresAdminService(),
        "kaggle_challenge",
        prepared,
        "-- manifest",
        "-- lock",
        workers,
        500_000,
    )


class ParallelExpansionTests(unittest.TestCase):
    def test_official_recipes_split_into_whole_source_and_per_block(self):
        runner = build_runner(prepare_official_recipes())

        self.assertEqual(
            [item["table_name"] for item in runner.whole_source],
            ["public.table3", "public.table4"],
        )
        self.assertEqual(
            [item["table_name"] for item in runner.per_block],
            ["public.table1", "public.table5", "public.table6", "public.table2"],
        )

    def test_block_statement_only_narrows_the_protected_source(self):
        prepared = prepare_official_recipes()
        runner = build_runner(prepared)
        statement = runner.per_block[0]["statements"][0]

        narrowed = runner._block_statement(statement, 10, 20)

        self.assertIn('AND "raw_id" BETWEEN 10 AND 20', narrowed)
        # Removing the block filter gives back the exact single-transaction statement.
        self.assertEqual(
            narrowed.replace('\n      AND "raw_id" BETWEEN 10 AND 20', ""),
            statement,
        )

    def test_block_transaction_is_idempotent_and_updates_counters(self):
        runner = build_runner(prepare_official_recipes())
        sql = runner._block_sql(
            7,
            {"block_number": 3, "raw_id_from": 1, "raw_id_to": 100},
        )

        self.assertIn("FOR UPDATE", sql)
        self.assertIn("IF pgdm_block_status = 'done' THEN", sql)
        self.assertIn("SET status = 'done'", sql)
        self.assertEqual(sql.count("pgdm_table_row_counts"), 4)
        self.assertLess(sql.index("SET status = 'done'"), sql.index("COMMIT;"))
        for _file_name, table_name in RECIPE_DESTINATIONS[2:]:
            self.assertIn(f"INSERT INTO {table_name}", sql)

    def test_joins_to_rows_of_the_same_block_use_nested_loops(self):
        runner = build_runner(prepare_official_recipes())
        sql = runner._block_sql(
            7,
            {"block_number": 3, "raw_id_from": 1, "raw_id_to": 100},
        )

        def join_settings_before(table_name):
            head = sql[: sql.index(f"INSERT INTO {table_name}")]
            return head[head.rindex("PERFORM set_config('enable_hashjoin'"):]

        # table1 only joins the whole-source dimension table3: planner default.
        self.assertIn("pgdm_enable_hashjoin", join_settings_before("public.table1"))
        for table_name in ("public.table5", "public.table6", "public.table2"):
            settings = join_settings_before(table_name)
            self.assertIn("'enable_hashjoin', 'off', true", settings)
            self.assertIn("'enable_mergejoin', 'off', true", settings)

    def test_aggregate_after_per_row_destination_is_rejected(self):
        prepared = prepare_official_recipes()
        prepared["destinations"] = [
            prepared["destinations"][2],
            prepared["destinations"][0],
        ]

        with self.assertRaises(RuntimeError):
            build_runner(prepared)

    def test_self_join_on_source_runs_on_whole_source(self):
        prepared = PostgresAdminService._prepare_cross_table_expansion(
            "public.raw_data",
            ["group001"],
            [
                {
                    "table_name": "public.table4",
                    "version_title": "Self join",
                    "sql": (
                        "INSERT INTO public.table4 (id_column_1) "
                        "SELECT a.raw_id::smallint FROM {{source}} AS a "
                        "JOIN {{source}} AS b ON b.raw_id = a.raw_id + 1"
                    ),
                }
            ],
        )

        runner = build_runner(prepared)

        self.assertEqual([item["table_name"] for item in runner.whole_source], ["public.table4"])
        self.assertEqual(runner.per_block, [])

    def test_fingerprint_changes_with_recipes_and_scope(self):
        prepared = prepare_official_recipes()
        base = build_runner(prepared).fingerprint
        changed = json.loads(json.dumps(prepared))
        changed["destinations"][5]["statements"][0] += " "
        rescoped = json.loads(json.dumps(prepared))
        rescoped["raw_schemas"] = ["group002"]

        self.assertEqual(base, build_runner(prepare_official_recipes()).fingerprint)
        self.assertNotEqual(base, build_runner(changed).fingerprint)
        self.assertNotEqual(base, build_runner(rescoped).fingerprint)


if __name__ == "__main__":
    unittest.main()
