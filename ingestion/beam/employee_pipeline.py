import argparse
import csv
import re
from datetime import date

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


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

    # Validate employee ID
    if not employee_id:
        return False

    if not employee_id.startswith("EMP"):
        return False

    # Validate email
    if not re.match(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        email,
    ):
        return False

    # Validate date of birth
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
# Transformation
# -----------------------------

def transform_record(record: dict) -> dict:
    """Transform a masked employee record into an analytics-ready record."""

    record = record.copy()

    # Standardize text fields
    record["department_id"] = (
        record["department_id"].strip().upper()
    )

    record["employment_status"] = (
        record["employment_status"].strip().title()
    )

    record["gender"] = (
        record["gender"].strip().title()
    )

    record["location"] = (
        record["location"].strip().title()
    )

    # Normalize date fields
    record["date_of_birth"] = (
        date.fromisoformat(
            record["date_of_birth"]
        ).isoformat()
    )

    record["hire_date"] = (
        date.fromisoformat(
            record["hire_date"]
        ).isoformat()
    )

    # Calculate employment duration
    hire_date = date.fromisoformat(
        record["hire_date"]
    )

    reference_date = date.today()

    employment_duration_years = (
        reference_date.year
        - hire_date.year
        - (
            (reference_date.month, reference_date.day)
            < (hire_date.month, hire_date.day)
        )
    )

    record["employment_duration_years"] = (
        employment_duration_years
    )

    return record


# -----------------------------
# Output Formatting
# -----------------------------

def format_output(record: dict) -> str:
    """Convert a standard employee record into a CSV row."""

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


def format_transformed_output(record: dict) -> str:
    """Convert a transformed employee record into a CSV row."""

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
            str(record["employment_duration_years"]),
        ]
    )


# -----------------------------
# Pipeline
# -----------------------------

def run(
    input_file: str,
    masked_output: str,
    invalid_output: str,
    pipeline_args: list[str],
) -> None:
    """Run the Apache Beam pipeline."""

    pipeline_options = PipelineOptions(
        pipeline_args
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
                input_file,
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
        # Transform records
        # -----------------------------

        transformed_records = (
            masked_records
            | "Transform Records" >> beam.Map(
                transform_record
            )
        )

        # -----------------------------
        # Format transformed output
        # -----------------------------

        masked_output_records = (
            transformed_records
            | "Format Masked Records" >> beam.Map(
                format_transformed_output
            )
        )

        # -----------------------------
        # Invalid records
        # -----------------------------

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

        masked_output_records | "Write Masked Records" >> beam.io.WriteToText(
            masked_output,
            file_name_suffix=".csv",
        )

        invalid_records | "Write Invalid Records" >> beam.io.WriteToText(
            invalid_output,
            file_name_suffix=".csv",
        )


# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_file",
        default=(
            "gs://secure-employee-data-platform-data/"
            "raw/hr/employees.csv"
        ),
    )

    parser.add_argument(
        "--masked_output",
        default=(
            "gs://secure-employee-data-platform-data/"
            "processed/masked_employees"
        ),
    )

    parser.add_argument(
        "--invalid_output",
        default=(
            "gs://secure-employee-data-platform-data/"
            "processed/invalid_employees"
        ),
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        input_file=known_args.input_file,
        masked_output=known_args.masked_output,
        invalid_output=known_args.invalid_output,
        pipeline_args=pipeline_args,
    )