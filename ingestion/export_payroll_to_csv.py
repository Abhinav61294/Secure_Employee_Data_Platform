import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()


# PostgreSQL connection configuration
DB_CONFIG = {
    "dbname": "payroll_db",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432,
}


OUTPUT_FILE = Path("data/payroll.csv")


def get_connection():
    """Create and return a PostgreSQL database connection."""

    return psycopg.connect(**DB_CONFIG)


def export_payroll() -> None:
    """Export payroll records from PostgreSQL to CSV."""

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT
                    employee_id,
                    salary,
                    bonus,
                    pay_grade,
                    bank_account_last4,
                    effective_date
                FROM payroll
                ORDER BY employee_id;
            """)

            rows = cursor.fetchall()

    fieldnames = [
        "employee_id",
        "salary",
        "bonus",
        "pay_grade",
        "bank_account_last4",
        "effective_date",
    ]

    with OUTPUT_FILE.open(
        mode="w",
        encoding="utf-8",
        newline="",
    ) as csv_file:

        writer = csv.writer(csv_file)

        writer.writerow(fieldnames)
        writer.writerows(rows)

    print(f"Exported {len(rows):,} payroll records.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    export_payroll()