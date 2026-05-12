"""bucket_upload_folder_ben — upload a local precomputed volume folder to a bucket.

Improvements over the upstream `bucket_upload_folder`:
  - Translates `___` in local filenames to ':' on the bucket side via direct `put`
    (the upstream version uses cf.move which fails on nokura — bulk DeleteObjects
    requires a Content-MD5 header that current botocore doesn't send).
  - Sets `ACL=public-read` on every uploaded object via boto3 (nokura objects
    default to private; browser fetches return 403 without this).
  - Applies a one-line patch to cloudfiles' TransmissionMonitor.end_io to tolerate
    zero-length intervals from sub-microsecond local IO on Windows.
  - Verifies every object with anonymous HTTPS HEAD and reports the canonical NG
    layer source URL.
"""
import os
import json
import argparse
import urllib.request

# Tolerate zero-length intervals on fast local reads (Windows).
from cloudfiles import monitoring as _cf_mon
_orig_end_io = _cf_mon.TransmissionMonitor.end_io
def _patched_end_io(self, flight_id, num_bytes_rx):
    try:
        return _orig_end_io(self, flight_id, num_bytes_rx)
    except ValueError:
        pass
_cf_mon.TransmissionMonitor.end_io = _patched_end_io

from cloudfiles import CloudFiles
import boto3
from botocore.client import Config


NOKURA_ENDPOINT_DEFAULT = "https://c10s.pni.princeton.edu/"
NOKURA_SECRET_PATH = os.path.join(os.path.expanduser("~"), ".cloudvolume", "secrets", "nokura-secret.json")


def _boto3_s3_client(secret_path=NOKURA_SECRET_PATH):
    """Build a boto3 S3 client using the nokura secret file."""
    with open(secret_path) as f:
        secret = json.load(f)
    access = secret.get("AWS_ACCESS_KEY_ID") or secret.get("aws_access_key_id") or secret.get("access_key")
    secret_key = secret.get("AWS_SECRET_ACCESS_KEY") or secret.get("aws_secret_access_key") or secret.get("secret_key")
    endpoint = secret.get("AWS_S3_ENDPOINT") or secret.get("endpoint_url") or NOKURA_ENDPOINT_DEFAULT
    return boto3.client(
        "s3",
        aws_access_key_id=access,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=Config(signature_version="s3v4"),
    ), endpoint


def _split_bucket_path(bucket_path):
    """Split 'nokura://tracers/ben/foo' into ('tracers', 'ben/foo')."""
    if "://" not in bucket_path:
        raise ValueError(f"expected protocol-prefixed bucket path, got: {bucket_path!r}")
    _, rest = bucket_path.split("://", 1)
    parts = rest.split("/", 1)
    bucket_name = parts[0]
    key_prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    return bucket_name, key_prefix


def _http_for_nokura(bucket_path, endpoint):
    """Map 'nokura://tracers/ben/foo' to 'https://endpoint/tracers/ben/foo'."""
    _, rest = bucket_path.split("://", 1)
    return endpoint.rstrip("/") + "/" + rest


def bucket_upload_folder_ben(
    local_path,
    bucket_path,
    public_read=True,
    verbose=True,
):
    """Upload a local folder (e.g. an `image/` precomputed volume) to a bucket.

    Translates `___` in local filenames back to ':' on the destination
    (the upstream `bucket_upload_folder` does the same via a cf.move that fails
    on nokura — this version uses direct puts with the final key name).

    Args:
      local_path: absolute path to the local folder to upload.
      bucket_path: bucket-style destination, e.g. 'nokura://tracers/ben/foo'.
      public_read: if True, set ACL=public-read on every uploaded object
                   (required for browser fetches from nokura).
      verbose: print per-file progress.

    Returns dict with keys:
      'bucket_path': the destination
      'https_url': anonymously-readable HTTPS base URL
      'uploaded': list of (local_rel_path, bucket_key) pairs
    """
    local_path = local_path.rstrip("/\\")
    bucket_path = bucket_path.rstrip("/")

    cf = CloudFiles(bucket_path)
    s3, endpoint = _boto3_s3_client() if public_read else (None, NOKURA_ENDPOINT_DEFAULT)
    bucket_name, key_prefix = _split_bucket_path(bucket_path)

    uploaded = []
    for root, _, files in os.walk(local_path):
        for fn in files:
            local_file = os.path.join(root, fn)
            rel = os.path.relpath(local_file, local_path).replace(os.sep, "/")
            bucket_key = rel.replace("___", ":")  # translate Windows-safe names

            with open(local_file, "rb") as f:
                data = f.read()
            # JSON-ish info / manifest files use put_json so Content-Type is correct.
            if bucket_key.endswith("info") or os.path.basename(bucket_key).count(":") == 1:
                try:
                    cf.put_json(bucket_key, json.loads(data))
                except (json.JSONDecodeError, ValueError):
                    cf.put(bucket_key, data, content_type="application/octet-stream")
            else:
                cf.put(bucket_key, data, content_type="application/octet-stream")

            if public_read:
                full_key = f"{key_prefix}/{bucket_key}" if key_prefix else bucket_key
                s3.put_object_acl(Bucket=bucket_name, Key=full_key, ACL="public-read")

            if verbose:
                print(f"  uploaded {rel} -> {bucket_key} ({len(data)} B)")
            uploaded.append((rel, bucket_key))

    https_url = _http_for_nokura(bucket_path, endpoint) if bucket_path.startswith("nokura://") else None

    if verbose and https_url:
        print(f"\nverifying public reads at {https_url} ...")
        ok = True
        for _, bucket_key in uploaded:
            url = f"{https_url}/{bucket_key}"
            try:
                with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=10) as resp:
                    if resp.status != 200:
                        ok = False
                        print(f"  {resp.status} {url}")
            except urllib.error.HTTPError as e:
                ok = False
                print(f"  {e.code} {url}")
        if ok:
            print("  all 200 OK")

    return {"bucket_path": bucket_path, "https_url": https_url, "uploaded": uploaded}


def ng_layer_source_for(https_url):
    """Build the canonical NG layer source URL (no '/' before '|')."""
    return f"{https_url.rstrip('/')}|neuroglancer-precomputed:"


def _cli():
    p = argparse.ArgumentParser(description="upload a local volume folder to a bucket with public-read ACLs")
    p.add_argument("local_path", help="local folder to upload (typically the 'image/' folder)")
    p.add_argument("bucket_path", help="destination, e.g. 'nokura://tracers/ben/<name>'")
    p.add_argument("--no-public", action="store_true", help="skip setting public-read ACL")
    args = p.parse_args()

    result = bucket_upload_folder_ben(
        local_path=args.local_path,
        bucket_path=args.bucket_path,
        public_read=not args.no_public,
    )
    if result["https_url"]:
        print(f"\nNG layer source:\n  {ng_layer_source_for(result['https_url'])}")
        print(f'  (state.json: "subsources": {{"bounds": true, "mesh": true}}, "enableDefaultSubsources": false)')


if __name__ == "__main__":
    _cli()
