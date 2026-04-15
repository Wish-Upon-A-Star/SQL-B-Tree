from __future__ import annotations

import csv
from pathlib import Path
import shutil
import subprocess


ROOT = Path(__file__).resolve().parent.parent
SOURCE_CSV = ROOT / "bptree_benchmark_users.csv"
WORKLOAD_TABLE = "jungle_workload_users"
WORKLOAD_CSV = ROOT / f"{WORKLOAD_TABLE}.csv"
OUTPUT_DIR = ROOT / "generated_sql"
INSERT_SQL = OUTPUT_DIR / "jungle_insert_1000000.sql"
UPDATE_SQL = OUTPUT_DIR / "jungle_update_1000000.sql"
DELETE_SQL = OUTPUT_DIR / "jungle_delete_1000000.sql"
NUMERIC_COLS = {0, 7}  # id, pretest
STATUS_TRANSITIONS = {
    "submitted": "pretest_pass",
    "pretest_pass": "interview_wait",
    "interview_wait": "final_wait",
    "final_wait": "final_pass",
    "final_pass": "final_pass",
    "rejected": "rejected",
    "withdrawn": "withdrawn",
}


def ensure_source_csv() -> None:
    if SOURCE_CSV.exists():
        return

    binary = ROOT / "sqlsprocessor"
    if binary.exists():
        subprocess.run(
            [str(binary), "--generate-jungle", "1000000", str(SOURCE_CSV)],
            check=True,
            cwd=ROOT,
        )
        return

    make_bin = shutil.which("make")
    if make_bin:
        subprocess.run([make_bin, "generate-jungle"], check=True, cwd=ROOT)
        return

    raise SystemExit(
        f"missing source csv: {SOURCE_CSV}\n"
        "run './sqlsprocessor --generate-jungle 1000000' or 'make generate-jungle' first"
    )


def sql_literal(value: str, col_index: int) -> str:
    if col_index in NUMERIC_COLS:
        return value
    return "'" + value.replace("'", "''") + "'"


def build_sql_workloads() -> None:
    with SOURCE_CSV.open("r", encoding="utf-8", newline="") as source_handle:
        reader = csv.reader(source_handle)
        header = next(reader)
        id_idx = header.index("id(PK)")
        status_idx = header.index("status")
        WORKLOAD_CSV.write_text(",".join(header) + "\n", encoding="utf-8")

        with INSERT_SQL.open("w", encoding="utf-8", newline="") as insert_handle, \
             UPDATE_SQL.open("w", encoding="utf-8", newline="") as update_handle, \
             DELETE_SQL.open("w", encoding="utf-8", newline="") as delete_handle:
            insert_handle.write("-- Generated from bptree_benchmark_users.csv\n")
            insert_handle.write("-- Requires jungle_workload_users.csv to exist with header only.\n")
            update_handle.write("-- One UPDATE statement per applicant row.\n")
            delete_handle.write("-- One DELETE statement per applicant row.\n")

            for row in reader:
                record_id = row[id_idx]
                current_status = row[status_idx]
                next_status = STATUS_TRANSITIONS.get(current_status, current_status)
                values = ", ".join(sql_literal(value, idx) for idx, value in enumerate(row))

                insert_handle.write(f"INSERT INTO {WORKLOAD_TABLE} VALUES ({values});\n")
                update_handle.write(
                    f"UPDATE {WORKLOAD_TABLE} SET status = '{next_status}' WHERE id = {record_id};\n"
                )
                delete_handle.write(
                    f"DELETE FROM {WORKLOAD_TABLE} WHERE id = {record_id};\n"
                )


def main() -> None:
    ensure_source_csv()

    OUTPUT_DIR.mkdir(exist_ok=True)

    build_sql_workloads()

    print(f"[ok] wrote {INSERT_SQL}")
    print(f"[ok] wrote {UPDATE_SQL}")
    print(f"[ok] wrote {DELETE_SQL}")
    print(f"[ok] reset base table header at {WORKLOAD_CSV}")


if __name__ == "__main__":
    main()
