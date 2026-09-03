import csv
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker


# -----------------------------
# Configuration
# -----------------------------

NUM_EMPLOYEES = 50_000
BAD_RECORD_RATE = 0.02
RANDOM_SEED = 42

OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "employees.csv"

fake = Faker("en_IN")
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


# -----------------------------
# Reference data
# -----------------------------

DEPARTMENTS = {
    "DEP001": "Engineering",
    "DEP002": "Data",
    "DEP003": "Finance",
    "DEP004": "Human Resources",
    "DEP005": "Sales",
    "DEP006": "Marketing",
    "DEP007": "Operations",
    "DEP008": "IT",
}

JOB_TITLES = [
    "Data Engineer",
    "Software Engineer",
    "Data Analyst",
    "Business Analyst",
    "Product Manager",
    "HR Manager",
    "Financial Analyst",
    "Sales Executive",
    "Marketing Manager",
    "Operations Analyst",
]

LOCATIONS = [
    "Pune",
    "Mumbai",
    "Bengaluru",
    "Hyderabad",
    "Delhi",
    "Chennai",
]

EMPLOYMENT_STATUSES = [
    "Active",
    "Active",
    "Active",
    "Active",
    "Leave",
    "Inactive",
]


# -----------------------------
# Helper functions
# -----------------------------

def random_date(start_year: int, end_year: int) -> date:
    """Generate a random date between two years."""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)

    days_between = (end - start).days

    return start + timedelta(
        days=random.randint(0, days_between)
    )


def generate_employee(employee_number: int) -> dict:
    """Generate one synthetic employee record."""

    first_name = fake.first_name()
    last_name = fake.last_name()

    department_id = random.choice(list(DEPARTMENTS.keys()))

    return {
        "employee_id": f"EMP{employee_number:05d}",
        "first_name": first_name,
        "last_name": last_name,
        "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
        "phone": fake.numerify("##########"),
        "date_of_birth": random_date(1970, 2002).isoformat(),
        "gender": random.choice(["Male", "Female", "Other"]),
        "department_id": department_id,
        "job_title": random.choice(JOB_TITLES),
        "location": random.choice(LOCATIONS),
        "hire_date": random_date(2015, 2026).isoformat(),
        "employment_status": random.choice(EMPLOYMENT_STATUSES),
    }


def introduce_bad_data(record: dict) -> dict:
    """Introduce a controlled data-quality issue."""

    issue = random.choice([
        "missing_employee_id",
        "invalid_email",
        "invalid_date",
    ])

    if issue == "missing_employee_id":
        record["employee_id"] = ""

    elif issue == "invalid_email":
        record["email"] = "invalid-email"

    elif issue == "invalid_date":
        record["date_of_birth"] = "not-a-date"

    return record


# -----------------------------
# Main generation process
# -----------------------------

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "employee_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "date_of_birth",
        "gender",
        "department_id",
        "job_title",
        "location",
        "hire_date",
        "employment_status",
    ]

    bad_records = int(NUM_EMPLOYEES * BAD_RECORD_RATE)

    bad_record_indexes = set(
        random.sample(
            range(NUM_EMPLOYEES),
            bad_records,
        )
    )

    with OUTPUT_FILE.open(
        mode="w",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames,
        )

        writer.writeheader()

        for index in range(NUM_EMPLOYEES):

            employee = generate_employee(index + 1)

            if index in bad_record_indexes:
                employee = introduce_bad_data(employee)

            writer.writerow(employee)

    print(f"Generated {NUM_EMPLOYEES:,} employee records.")
    print(f"Injected {bad_records:,} bad records.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()