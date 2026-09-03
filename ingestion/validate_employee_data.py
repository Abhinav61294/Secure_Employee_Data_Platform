import csv
import re
from pathlib import Path


INPUT_FILE = Path("data/employees.csv")

REQUIRED_COLUMNS = {
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
}

VALID_DEPARTMENTS = {
    "DEP001",
    "DEP002",
    "DEP003",
    "DEP004",
    "DEP005",
    "DEP006",
    "DEP007",
    "DEP008",
}

VALID_STATUSES = {
    "Active",
    "Inactive",
    "Leave",
}

EMAIL_PATTERN = re.compile(
    r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
)


def validate_email(email: str) -> bool:
    """Return True if the email has a valid basic format."""
    return bool(EMAIL_PATTERN.match(email))


def validate_date(date_value: str) -> bool:
    """Return True if the value follows YYYY-MM-DD."""
    if len(date_value) != 10:
        return False

    try:
        year, month, day = map(int, date_value.split("-"))

        return (
            1900 <= year <= 2100
            and 1 <= month <= 12
            and 1 <= day <= 31
        )

    except ValueError:
        return False


def validate_employee_data() -> dict:
    """Validate the employee CSV and return validation results."""

    results = {
        "total_records": 0,
        "missing_employee_id": 0,
        "invalid_email": 0,
        "invalid_date_of_birth": 0,
        "invalid_department": 0,
        "invalid_status": 0,
        "duplicate_employee_id": 0,
    }

    employee_ids = set()

    with INPUT_FILE.open(
        mode="r",
        encoding="utf-8",
        newline="",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        # Validate schema
        actual_columns = set(reader.fieldnames or [])

        missing_columns = REQUIRED_COLUMNS - actual_columns

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:

            results["total_records"] += 1

            employee_id = row["employee_id"]

            # Employee ID validation
            if not employee_id:
                results["missing_employee_id"] += 1
            elif employee_id in employee_ids:
                results["duplicate_employee_id"] += 1
            else:
                employee_ids.add(employee_id)

            # Email validation
            if not validate_email(row["email"]):
                results["invalid_email"] += 1

            # Date validation
            if not validate_date(row["date_of_birth"]):
                results["invalid_date_of_birth"] += 1

            # Department validation
            if row["department_id"] not in VALID_DEPARTMENTS:
                results["invalid_department"] += 1

            # Employment status validation
            if row["employment_status"] not in VALID_STATUSES:
                results["invalid_status"] += 1

    return results


def main() -> None:
    results = validate_employee_data()

    print("\nEMPLOYEE DATA VALIDATION")
    print("=" * 30)

    print(f"Records checked:        {results['total_records']:,}")
    print(
        f"Missing employee IDs:  "
        f"{results['missing_employee_id']:,}"
    )
    print(
        f"Invalid emails:        "
        f"{results['invalid_email']:,}"
    )
    print(
        f"Invalid DOBs:          "
        f"{results['invalid_date_of_birth']:,}"
    )
    print(
        f"Invalid departments:   "
        f"{results['invalid_department']:,}"
    )
    print(
        f"Invalid statuses:      "
        f"{results['invalid_status']:,}"
    )
    print(
        f"Duplicate employee IDs:"
        f" {results['duplicate_employee_id']:,}"
    )

    total_errors = sum(
        value
        for key, value in results.items()
        if key != "total_records"
    )

    print("\n" + "=" * 30)

    if total_errors == 0:
        print("Status: PASSED")
    else:
        print(f"Status: FAILED ({total_errors:,} issues found)")


if __name__ == "__main__":
    main()