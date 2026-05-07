"""MinIO file existence check task."""

import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET   = "raw"

_REQUIRED_PREFIXES = [
    "airbyte/mysql/customers/",
    "airbyte/mysql/orders/",
    "airbyte/mysql/products/",
    "airbyte/sap/vendors/",
    "airbyte/sap/purchase_orders/",
    "airbyte/rest/users/",
    "airbyte/rest/posts/",
]


def wait_for_minio_files(**ctx):
    """Verify all expected Parquet prefixes exist in MinIO raw bucket."""
    try:
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}") from e

    client  = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    missing = []

    for prefix in _REQUIRED_PREFIXES:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
        if objects:
            print(f"  OK  {MINIO_BUCKET}/{prefix}  ({len(objects)} file(s))")
        else:
            print(f"  MISSING  {MINIO_BUCKET}/{prefix}")
            missing.append(prefix)

    if missing:
        raise AssertionError(f"Missing MinIO paths: {missing}")

    print("All required MinIO paths verified.")
