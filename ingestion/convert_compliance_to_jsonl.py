import json
from pathlib import Path


INPUT_FILE = Path("data/employee_compliance.json")
OUTPUT_FILE = Path("data/employee_compliance.jsonl")


def convert_to_jsonl() -> None:
    """Convert a JSON array into newline-delimited JSON."""

    with INPUT_FILE.open(
        mode="r",
        encoding="utf-8",
    ) as input_file:
        records = json.load(input_file)

    with OUTPUT_FILE.open(
        mode="w",
        encoding="utf-8",
    ) as output_file:

        for record in records:
            json.dump(record, output_file)
            output_file.write("\n")

    print(f"Converted {len(records):,} records.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    convert_to_jsonl()