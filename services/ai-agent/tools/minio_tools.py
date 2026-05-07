"""
MinIO tools for the AI agent.

Singleton client: a single module-level Minio instance is reused across all
tool calls within a container process. The Minio client is stateless and
thread-safe — reusing it avoids the overhead of DNS resolution and TLS
handshake on every tool invocation.
"""

import os
import threading

from langchain_core.tools import tool
from minio import Minio
from minio.error import S3Error


# ── Singleton client ──────────────────────────────────────────────────────────

_minio: Minio | None = None
_minio_lock = threading.Lock()


def _client() -> Minio:
    global _minio
    if _minio is None:
        with _minio_lock:
            if _minio is None:
                _minio = Minio(
                    f"{os.getenv('MINIO_HOST', 'minio')}:{os.getenv('MINIO_PORT', '9000')}",
                    access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
                    secret_key=os.getenv("MINIO_SECRET_KEY", ""),
                    secure=False,
                )
    return _minio


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def list_minio_files(prefix: str = "airbyte/") -> str:
    """List files in the MinIO 'raw' bucket under the given prefix.
    Use to verify Airbyte sync results landed (e.g. prefix='airbyte/mysql/')."""
    try:
        objects = list(_client().list_objects("raw", prefix=prefix, recursive=True))
        if not objects:
            return f"No files found in raw/{prefix}"
        lines = [
            f"  {o.object_name}  ({o.size / 1024:.1f} KB, {o.last_modified.date()})"
            for o in objects[:20]
        ]
        suffix = f"\n  ... and {len(objects) - 20} more" if len(objects) > 20 else ""
        return f"Files in raw/{prefix}:\n" + "\n".join(lines) + suffix
    except S3Error as e:
        return f"MinIO S3 error: {str(e)}"
    except Exception as e:
        return f"MinIO error: {str(e)}"


@tool
def check_minio_bucket_size(bucket: str = "raw") -> str:
    """Get the total file count and size for a MinIO bucket.
    Use for storage capacity monitoring."""
    try:
        objects = list(_client().list_objects(bucket, recursive=True))
        total_bytes = sum(o.size for o in objects)
        mb = total_bytes / 1024 / 1024
        return (
            f"Bucket '{bucket}': {len(objects)} files, "
            f"{mb:.1f} MB total"
            + (" ⚠ > 1 GB — consider archiving" if mb > 1024 else "")
        )
    except Exception as e:
        return f"Error checking bucket: {str(e)}"


@tool
def list_minio_buckets(max_buckets: str = "50") -> str:
    """List all MinIO buckets (up to max_buckets, e.g. "50"). Use to confirm bucket structure is correct."""
    try:
        buckets = _client().list_buckets()
        lines = [f"  {b.name} (created {b.creation_date.date()})" for b in buckets]
        return "MinIO buckets:\n" + "\n".join(lines)
    except Exception as e:
        return f"Error listing buckets: {str(e)}"
