import csv
from datetime import datetime

FILE = "data/employees.csv"

with open(FILE, encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)

    bad_dob = []
    bad_hire = []

    for line_number, row in enumerate(reader, start=2):
        try:
            datetime.strptime(row["date_of_birth"], "%Y-%m-%d")
        except ValueError:
            bad_dob.append(
                (line_number, row["date_of_birth"])
            )

        try:
            datetime.strptime(row["hire_date"], "%Y-%m-%d")
        except ValueError:
            bad_hire.append(
                (line_number, row["hire_date"])
            )

print("Bad date_of_birth:", bad_dob[:20])
print("Bad hire_date:", bad_hire[:20])