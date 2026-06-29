"""bucket_copy_folder_ben — bucket-to-bucket copy of a precomputed folder.

Local stand-in for Jay's `tracer_tools.bucket_copy_folder` (which, as of
2026-06-29, isn't pushed to any tracer_tools repo we can `git pull`). Same
intent: copy one mesh folder to another bucket location. Uses our nokura-safe
machinery from bucket_upload_folder_ben:

  - Streams each object source -> dest in memory (no local temp files, so the
    Windows ':'-in-filename problem never arises; bucket keys keep their colons).
  - Sets ACL=public-read on every destination object via boto3 (nokura objects
    default to private; browser fetches 403 without this).
  - Verifies every destination object with an anonymous HTTPS HEAD.

Importing bucket_upload_folder_ben also applies the TransmissionMonitor.end_io
patch for sub-microsecond IO on Windows.
"""
import os
import sys
import json
import argparse
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bucket_upload_folder_ben as _up  # noqa: E402  (also applies the cloudfiles patch)
from cloudfiles import CloudFiles  # noqa: E402


def bucket_copy_folder_ben(source_path, dest_path, public_read=True, verbose=True):
    """Copy every object under source_path to dest_path on a bucket.

    Args:
      source_path: e.g. 'nokura://tracers/ben/Left_CB_Tear'
      dest_path:   e.g. 'nokura://tracers/swamps/banc/individual_meshes/11'
      public_read: set ACL=public-read on each destination object.
      verbose:     per-file progress.

    Returns dict: {'source','dest','https_url','copied':[(key,nbytes),...]}.
    """
    source_path = source_path.rstrip("/")
    dest_path = dest_path.rstrip("/")

    cf_src = CloudFiles(source_path)
    cf_dst = CloudFiles(dest_path)
    s3, endpoint = _up._boto3_s3_client() if public_read else (None, _up.NOKURA_ENDPOINT_DEFAULT)
    dst_bucket, dst_prefix = _up._split_bucket_path(dest_path)

    keys = sorted(cf_src.list())
    if not keys:
        raise ValueError(f"no objects found under {source_path!r}")

    copied = []
    for key in keys:
        data = cf_src.get(key)
        # Mirror the upload helper: info + single-colon manifest files are JSON.
        is_json = key.endswith("info") or os.path.basename(key).count(":") == 1
        if is_json:
            try:
                cf_dst.put_json(key, json.loads(data))
            except (json.JSONDecodeError, ValueError):
                cf_dst.put(key, data, content_type="application/octet-stream")
        else:
            cf_dst.put(key, data, content_type="application/octet-stream")

        if public_read:
            full_key = f"{dst_prefix}/{key}" if dst_prefix else key
            s3.put_object_acl(Bucket=dst_bucket, Key=full_key, ACL="public-read")

        if verbose:
            print(f"  copied {key} -> {dest_path}/{key} ({len(data)} B)")
        copied.append((key, len(data)))

    https_url = _up._http_for_nokura(dest_path, endpoint) if dest_path.startswith("nokura://") else None

    if verbose and https_url:
        print(f"\nverifying public reads at {https_url} ...")
        ok = True
        for key, _ in copied:
            url = f"{https_url}/{key}"
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

    return {"source": source_path, "dest": dest_path, "https_url": https_url, "copied": copied}


def _cli():
    p = argparse.ArgumentParser(description="copy a precomputed folder between bucket locations (nokura-safe)")
    p.add_argument("source_path")
    p.add_argument("dest_path")
    p.add_argument("--no-public", action="store_true", help="skip setting public-read ACL")
    args = p.parse_args()
    bucket_copy_folder_ben(args.source_path, args.dest_path, public_read=not args.no_public)


if __name__ == "__main__":
    _cli()
