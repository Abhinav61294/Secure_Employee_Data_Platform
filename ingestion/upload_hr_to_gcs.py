from pathlib import Path

from google.cloud import storage


BUCKET_NAME = "secure-employee-data-platform-data"
SOURCE_FILE = Path("data/employees.csv")
DESTINATION_BLOB = "raw/hr/employees.csv"


def upload_file():
    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB)

    blob.upload_from_filename(SOURCE_FILE)

    print("Upload successful")
    print(f"Source: {SOURCE_FILE}")
    print(f"Destination: gs://{BUCKET_NAME}/{DESTINATION_BLOB}")


if __name__ == "__main__":
    upload_file()