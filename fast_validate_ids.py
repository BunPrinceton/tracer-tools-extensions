#!/usr/bin/env python3
"""
Fast parallel ID validator - processes 5000+ IDs in under 2 minutes.

Uses ThreadPoolExecutor to parallelize supervoxel lookups (the bottleneck),
then batches the root lookups in a single API call.

Usage:
    python fast_validate_ids.py --input ids.txt
    python fast_validate_ids.py --input ids.txt --output results.txt --workers 20
"""

import argparse
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add tracer_tools to path
for candidate in [
    Path(__file__).parent / "Tracer - Workspace" / "tracer_tools" / "src",
    Path(__file__).parent.parent / "tracer_tools" / "src",
    Path(r"C:\Users\Benjamin\Desktop\Tracer - Workspace\tracer_tools\src"),
    Path(r"C:\E_Drive\1337\tracer_tools_fresh\src"),
]:
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break

from caveclient import CAVEclient


def parse_id_file(filepath):
    """Parse ID file - handles plain IDs and N->ID format."""
    ids = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "\u2192" in line:  # arrow character
                parts = line.split("\u2192")
                id_part = parts[-1].strip()
                if id_part and id_part.isdigit():
                    ids.append(id_part)
            elif line.isdigit():
                ids.append(line)
    return ids


def get_one_supervoxel(client, root_id):
    """Get one supervoxel from a root ID. Runs in a thread."""
    try:
        sv_ids = client.chunkedgraph.get_leaves(int(root_id))
        if len(sv_ids) > 0:
            return (root_id, sv_ids[0], None)
        return (root_id, None, "No supervoxels found")
    except Exception as e:
        return (root_id, None, str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Fast parallel ID validator (ThreadPoolExecutor)"
    )
    parser.add_argument("--input", "-i", required=True, help="Input file with IDs")
    parser.add_argument("--output", "-o", help="Output file (default: input_updated.txt)")
    parser.add_argument(
        "--datastack", "-d", default="brain_and_nerve_cord", help="Datastack name"
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=20, help="Parallel workers (default: 20)"
    )

    args = parser.parse_args()

    # Parse IDs
    print("Reading IDs...")
    all_ids = parse_id_file(args.input)
    print(f"Found {len(all_ids)} IDs to validate")

    if not all_ids:
        print("No IDs found!", file=sys.stderr)
        sys.exit(1)

    if not args.output:
        input_path = Path(args.input)
        args.output = str(input_path.parent / f"{input_path.stem}_updated.txt")

    start_time = time.time()

    # Create one shared client for all threads
    client = CAVEclient(datastack_name=args.datastack)

    # ---- STEP 1: Parallel supervoxel lookups ----
    print(f"\nStep 1: Getting supervoxels ({args.workers} parallel workers)...")
    id_to_sv = {}
    errors = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(get_one_supervoxel, client, rid): rid
            for rid in all_ids
        }
        for future in as_completed(futures):
            root_id, sv, err = future.result()
            if sv is not None:
                id_to_sv[root_id] = sv
            else:
                errors[root_id] = err
            completed += 1
            if completed % 500 == 0 or completed == len(all_ids):
                elapsed = time.time() - start_time
                print(f"  {completed}/{len(all_ids)} supervoxels fetched ({elapsed:.1f}s)")

    sv_time = time.time() - start_time
    print(f"  Done: {len(id_to_sv)} supervoxels in {sv_time:.1f}s ({len(errors)} errors)")

    # ---- STEP 2: Batched root lookups ----
    print("\nStep 2: Batch looking up current roots...")
    step2_start = time.time()

    sv_list = list(id_to_sv.values())

    # Chunk into batches of 5000 to avoid API limits
    chunk_size = 5000
    sv_to_root = {}
    for i in range(0, len(sv_list), chunk_size):
        chunk = sv_list[i : i + chunk_size]
        new_roots = client.chunkedgraph.get_roots(chunk)
        for sv, root in zip(chunk, new_roots):
            sv_to_root[sv] = root
        print(f"  Roots batch {i // chunk_size + 1}: {len(chunk)} looked up")

    step2_time = time.time() - step2_start
    print(f"  Done in {step2_time:.1f}s")

    # ---- STEP 3: Build results ----
    print("\nStep 3: Building results...")
    lines = []
    changed_count = 0
    unchanged_count = 0
    error_count = 0
    updated_ids = []

    for old_id in all_ids:
        if old_id in errors:
            lines.append(f"# ERROR: {old_id} - {errors[old_id]}")
            error_count += 1
            updated_ids.append(old_id)
            continue

        sv = id_to_sv.get(old_id)
        if sv is not None and sv in sv_to_root:
            new_id = str(sv_to_root[sv])
            if new_id != old_id:
                changed_count += 1
                lines.append(f"{old_id}    ->    {new_id}")
                updated_ids.append(new_id)
            else:
                unchanged_count += 1
                lines.append(f"{old_id}    [OK - Current]")
                updated_ids.append(old_id)
        else:
            lines.append(f"# ERROR: {old_id} - lookup failed")
            error_count += 1
            updated_ids.append(old_id)

    # ---- Write report ----
    total_time = time.time() - start_time
    with open(args.output, "w") as f:
        f.write(f"# ID Validation Report\n")
        f.write(f"# Total IDs: {len(all_ids)}\n")
        f.write(f"# Changed: {changed_count}\n")
        f.write(f"# Unchanged: {unchanged_count}\n")
        f.write(f"# Errors: {error_count}\n")
        f.write(f"# Time: {total_time:.1f}s\n")
        f.write(f"# Workers: {args.workers}\n")
        f.write(f"\n---RESULTS---\n\n")
        f.write("\n".join(lines))

    # ---- Write clean updated IDs file ----
    clean_output = str(Path(args.output).parent / f"{Path(args.output).stem}_clean.txt")
    with open(clean_output, "w") as f:
        f.write("\n".join(updated_ids))

    # ---- Summary ----
    print(f"\n{'=' * 60}")
    print(f"VALIDATION COMPLETE - {total_time:.1f} seconds")
    print(f"{'=' * 60}")
    print(f"Total IDs:  {len(all_ids)}")
    print(f"Changed:    {changed_count} ({changed_count / len(all_ids) * 100:.1f}%)")
    print(f"Unchanged:  {unchanged_count} ({unchanged_count / len(all_ids) * 100:.1f}%)")
    print(f"Errors:     {error_count}")
    print(f"Report:     {args.output}")
    print(f"Clean IDs:  {clean_output}")


if __name__ == "__main__":
    main()
