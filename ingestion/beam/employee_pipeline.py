import csv
import re
from datetime import date

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# -----------------------------
# Configuration
# -----------------------------

# Temporary test file containing duplicate records
INPUT_FILE = "data/employees.csv"               # Switched from beam_test_duplicates.

VALID_OUTPUT = "data/beam_output/valid_employees"
INVALID_OUTPUT = "data/beam_output/invalid_employees"
MASKED_OUTPUT = "data/beam_output/masked_employees"


# -----------------------------
# CSV Parsing
# -----------------------------

def parse_csv(line: str) -> dict:
    """Convert a CSV line into a dictionary."""

    values = next(csv.reader([line]))

    return {
        "employee_id": values[0],
        "first_name": values[1],
        "last_name": values[2],
        "email": values[3],
        "phone": values[4],
        "date_of_birth": values[5],
        "gender": values[6],
        "department_id": values[7],
        "job_title": values[8],
        "location": values[9],
        "hire_date": values[10],
        "employment_status": values[11],
    }


# -----------------------------
# Data Validation
# -----------------------------

def is_valid_employee(record: dict) -> bool:
    """Validate required employee fields."""

    employee_id = record["employee_id"]
    email = record["email"]
    date_of_birth = record["date_of_birth"]

    if not employee_id:
        return False

    if not employee_id.startswith("EMP"):
        return False

    if not re.match(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        email,
    ):
        return False

    try:
        date.fromisoformat(date_of_birth)
    except ValueError:
        return False

    return True


# -----------------------------
# Deduplication
# -----------------------------

def deduplicate_records(keyed_records: tuple) -> dict:
    """Keep the first record for each employee_id."""

    employee_id, records = keyed_records

    records = list(records)

    return records[0]


# -----------------------------
# PII Masking
# -----------------------------

def mask_text(value: str) -> str:
    """Mask a text value while keeping its first character."""

    if not value:
        return value

    if len(value) == 1:
        return "*"

    return value[0] + "*" * (len(value) - 1)


def mask_email(email: str) -> str:
    """Mask the local part of an email address."""

    if "@" not in email:
        return "***"

    local_part, domain = email.split("@", 1)

    if not local_part:
        return "***@" + domain

    return local_part[0] + "***@" + domain


def mask_phone(phone: str) -> str:
    """Mask all but the last four digits of a phone number."""

    if len(phone) <= 4:
        return "*" * len(phone)

    return "*" * (len(phone) - 4) + phone[-4:]


def mask_pii(record: dict) -> dict:
    """Mask sensitive employee information."""

    record = record.copy()

    record["first_name"] = mask_text(record["first_name"])
    record["last_name"] = mask_text(record["last_name"])
    record["email"] = mask_email(record["email"])
    record["phone"] = mask_phone(record["phone"])

    return record


# -----------------------------
# Output Formatting
# -----------------------------

def format_output(record: dict) -> str:
    """Convert a dictionary back into a CSV row."""

    return ",".join(
        [
            record["employee_id"],
            record["first_name"],
            record["last_name"],
            record["email"],
            record["phone"],
            record["date_of_birth"],
            record["gender"],
            record["department_id"],
            record["job_title"],
            record["location"],
            record["hire_date"],
            record["employment_status"],
        ]
    )


# -----------------------------
# Pipeline
# -----------------------------

def run() -> None:
    """Run the Apache Beam pipeline."""

    pipeline_options = PipelineOptions(
        runner="DirectRunner"
    )

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        # -----------------------------
        # Read and parse source data
        # -----------------------------

        records = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                INPUT_FILE,
                skip_header_lines=1,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        # -----------------------------
        # Validate records
        # -----------------------------

        valid_records = (
            records
            | "Validate Records" >> beam.Filter(
                is_valid_employee
            )
        )

        # -----------------------------
        # Deduplicate valid records
        # -----------------------------

        deduplicated_records = (
            valid_records
            | "Key By Employee ID" >> beam.Map(
                lambda record: (
                    record["employee_id"],
                    record,
                )
            )
            | "Group By Employee ID" >> beam.GroupByKey()
            | "Deduplicate Records" >> beam.Map(
                deduplicate_records
            )
        )

        # -----------------------------
        # Mask PII
        # -----------------------------

        masked_records = (
            deduplicated_records
            | "Mask PII" >> beam.Map(
                mask_pii
            )
        )

        # -----------------------------
        # Format outputs
        # -----------------------------

        masked_output = (
            masked_records
            | "Format Masked Records" >> beam.Map(
                format_output
            )
        )

        invalid_records = (
            records
            | "Find Invalid Records" >> beam.Filter(
                lambda record: not is_valid_employee(record)
            )
            | "Format Invalid Records" >> beam.Map(
                format_output
            )
        )

        # -----------------------------
        # Write outputs
        # -----------------------------

        masked_output | "Write Masked Records" >> beam.io.WriteToText(
            MASKED_OUTPUT,
            file_name_suffix=".csv",
        )

        invalid_records | "Write Invalid Records" >> beam.io.WriteToText(
            INVALID_OUTPUT,
            file_name_suffix=".csv",
        )


# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    run()