import csv
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


INPUT_FILE = "data/employees.csv"

VALID_OUTPUT = "data/beam_output/valid_employees"
INVALID_OUTPUT = "data/beam_output/invalid_employees"


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
        from datetime import date

        date.fromisoformat(date_of_birth)
    except ValueError:
        return False

    return True


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


def run() -> None:
    """Run the Apache Beam pipeline."""

    pipeline_options = PipelineOptions(
        runner="DirectRunner"
    )

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        records = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                INPUT_FILE,
                skip_header_lines=1,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        valid_records = (
            records
            | "Validate Records" >> beam.Filter(
                is_valid_employee
            )
            | "Format Valid Records" >> beam.Map(
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

        valid_records | "Write Valid Records" >> beam.io.WriteToText(
            VALID_OUTPUT,
            file_name_suffix=".csv",
        )

        invalid_records | "Write Invalid Records" >> beam.io.WriteToText(
            INVALID_OUTPUT,
            file_name_suffix=".csv",
        )


if __name__ == "__main__":
    run()