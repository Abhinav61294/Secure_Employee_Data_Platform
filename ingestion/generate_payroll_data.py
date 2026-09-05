import csv
import os
import random
from datetime import date, timedelta
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


# Input HR data
HR_FILE = Path("data/employees.csv")


def get_connection():
    """Create and return a PostgreSQL database connection."""

    return psycopg.connect(**DB_CONFIG)


def load_employee_ids() -> list[str]:
    """Load valid employee IDs from the HR CSV."""

    employee_ids = []

    with HR_FILE.open(
        mode="r",
        encoding="utf-8",
        newline="",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        for row in reader:
            employee_id = row["employee_id"]

            if employee_id:
                employee_ids.append(employee_id)

    return employee_ids


def generate_payroll_record(employee_id: str) -> tuple:
    """Generate a synthetic payroll record."""

    # Generate annual salary between ₹30,000 and ₹250,000
    salary = random.randint(30000, 250000)

    # Generate bonus between 0% and 20% of salary
    bonus = round(
        salary * random.uniform(0, 0.20),
        2,
    )

    # Generate pay grade
    pay_grade = random.choice(
        [
            "G1",
            "G2",
            "G3",
            "G4",
            "G5",
            "G6",
            "G7",
            "G8",
        ]
    )

    # Generate a synthetic bank account last-four value
    bank_account_last4 = f"{random.randint(0, 9999):04d}"

    # Generate an effective date between 2020 and 2026
    start_date = date(2020, 1, 1)
    end_date = date(2026, 9, 1)

    days_between = (end_date - start_date).days

    effective_date = start_date + timedelta(
        days=random.randint(0, days_between)
    )

    return (
        employee_id,
        salary,
        bonus,
        pay_grade,
        bank_account_last4,
        effective_date,
    )

def insert_payroll_records(records: list[tuple]) -> None:
    """Insert payroll records into PostgreSQL."""

    insert_query = """
        INSERT INTO payroll (
            employee_id,
            salary,
            bonus,
            pay_grade,
            bank_account_last4,
            effective_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (employee_id) DO UPDATE SET
    salary = EXCLUDED.salary,
    bonus = EXCLUDED.bonus,
    pay_grade = EXCLUDED.pay_grade,
    bank_account_last4 = EXCLUDED.bank_account_last4,
    effective_date = EXCLUDED.effective_date
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                insert_query,
                records,
            )

        connection.commit()

    print(f"Inserted {len(records):,} payroll records")


if __name__ == "__main__":
    employee_ids = load_employee_ids()

    print(f"Loaded {len(employee_ids):,} employee IDs")

    payroll_records = [
        generate_payroll_record(employee_id)
        for employee_id in employee_ids
    ]

    print(f"Generated {len(payroll_records):,} payroll records")

    print("\nSample payroll record:")
print(payroll_records[0])

insert_payroll_records(payroll_records)