from google.cloud import storage


BUCKET_NAME = "secure-employee-data-platform-data"


def main():
    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)

    if bucket.exists():
        print(f"Connected to bucket: {BUCKET_NAME}")
    else:
        print(f"Bucket not found: {BUCKET_NAME}")


if __name__ == "__main__":
    main()