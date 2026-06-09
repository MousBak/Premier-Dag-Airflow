"""
Create MinIO buckets required by the pipeline.
Usage: python scripts/create_buckets.py
"""

import sys
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ENDPOINT  = "http://localhost:9000"
ACCESS    = "minioadmin"
SECRET    = "minioadmin"
BUCKETS   = ["meteo-bronze", "meteo-silver"]


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        config=Config(signature_version="s3v4"),
    )

    for bucket in BUCKETS:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"  Created : {bucket}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"  Exists  : {bucket} (skipped)")
            else:
                print(f"  ERROR   : {bucket} — {e}", file=sys.stderr)
                sys.exit(1)

    print("\nDone. Buckets ready:")
    for obj in s3.list_buckets().get("Buckets", []):
        print(f"  - {obj['Name']}")


if __name__ == "__main__":
    main()
