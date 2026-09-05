import json
from pathlib import Path

import httpx


API_URL = "http://127.0.0.1:8000"

HR_FILE = Path("data/employees.csv")
OUTPUT_FILE = Path(
    "data/employee_compliance.json"
)


def load_employee_ids() -> list[str]:
    """Load valid employee IDs from the HR CSV."""

    import csv

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


def fetch_compliance_data(
    employee_ids: list[str],
) -> list[dict]:
    """Fetch compliance information from the REST API."""

    records = []

    with httpx.Client(timeout=10.0) as client:

        for employee_id in employee_ids:
            url = f"{API_URL}/employees/{employee_id}"

            response = client.get(url)

            response.raise_for_status()

            records.append(response.json())

    return records


def save_records(records: list[dict]) -> None:
    """Save API records as JSON."""

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_FILE.open(
        mode="w",
        encoding="utf-8",
    ) as output_file:

        json.dump(
            records,
            output_file,
            indent=4,
        )


if __name__ == "__main__":
    employee_ids = load_employee_ids()

    print(
        f"Loaded {len(employee_ids):,} employee IDs"
    )

    records = fetch_compliance_data(employee_ids)

    print(
        f"Fetched {len(records):,} compliance records"
    )

    save_records(records)

    print(f"Output: {OUTPUT_FILE}")