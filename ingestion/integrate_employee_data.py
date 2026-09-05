import csv
from pathlib import Path
import os
import json
import psycopg
from dotenv import load_dotenv

HR_FILE = Path("data/employees.csv")

load_dotenv()


DB_CONFIG = {
    "dbname": "payroll_db",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432,
}

def load_payroll_data() -> dict[str, dict]:
    """Load payroll records from PostgreSQL."""

    payroll_data = {}

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    employee_id,
                    salary,
                    bonus,
                    pay_grade,
                    bank_account_last4,
                    effective_date
                FROM payroll
                """
            )

            rows = cursor.fetchall()

    for row in rows:
        (
            employee_id,
            salary,
            bonus,
            pay_grade,
            bank_account_last4,
            effective_date,
        ) = row

        payroll_data[employee_id] = {
            "salary": float(salary),
            "bonus": float(bonus),
            "pay_grade": pay_grade,
            "bank_account_last4": bank_account_last4.strip(),
            "effective_date": effective_date.isoformat(),
        }

    return payroll_data

COMPLIANCE_FILE = Path(
    "data/employee_compliance.json"
)


def load_compliance_data() -> dict[str, dict]:
    """Load employee compliance data from JSON."""

    with COMPLIANCE_FILE.open(
        mode="r",
        encoding="utf-8",
    ) as json_file:

        records = json.load(json_file)

    compliance_data = {}

    for record in records:
        employee_id = record["employee_id"]

        compliance_data[employee_id] = {
            "compliance_status": record["compliance_status"],
            "background_check_status": record[
                "background_check_status"
            ],
            "training_status": record["training_status"],
            "last_review_date": record["last_review_date"],
        }

    return compliance_data

def load_employee_data() -> list[dict]:
    """Load employee records from the HR CSV."""

    records = []

    with HR_FILE.open(
        mode="r",
        encoding="utf-8",
        newline="",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        for row in reader:
            records.append(row)

    return records
def integrate_employee_data(
    employees: list[dict],
    payroll: dict[str, dict],
    compliance: dict[str, dict],
) -> list[dict]:
    """Combine HR, payroll, and compliance data."""

    integrated_records = []

    for employee in employees:

        employee_id = employee["employee_id"]

        if not employee_id:
            continue

        payroll_record = payroll.get(employee_id)
        compliance_record = compliance.get(employee_id)

        if payroll_record is None:
            continue

        if compliance_record is None:
            continue

        integrated_record = {
            **employee,
            **payroll_record,
            **compliance_record,
        }

        integrated_records.append(
            integrated_record
        )

    return integrated_records
def save_integrated_data(
    records: list[dict],
) -> None:
    """Save integrated employee data as JSON."""

    output_file = Path(
        "data/integrated_employee_data.json"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_file.open(
        mode="w",
        encoding="utf-8",
    ) as json_file:

        json.dump(
            records,
            json_file,
            indent=4,
            default=str,
        )

    print(f"Output: {output_file}")

if __name__ == "__main__":
    employees = load_employee_data()

    print(
        f"Loaded {len(employees):,} employee records"
    )

    payroll = load_payroll_data()

    print(
        f"Loaded {len(payroll):,} payroll records"
    )

    compliance = load_compliance_data()

    print(
        f"Loaded {len(compliance):,} compliance records"
    )

    integrated = integrate_employee_data(
        employees,
        payroll,
        compliance,
    )

    print(
        f"\nIntegrated records: "
        f"{len(integrated):,}"
    )

    print("\nSample integrated record:")
    print(integrated[0])
    save_integrated_data(integrated)