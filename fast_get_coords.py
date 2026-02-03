#!/usr/bin/env python3
"""
Fast parallel coordinate fetcher - gets coords for thousands of IDs quickly.

Uses ThreadPoolExecutor to parallelize L2 lookups (the bottleneck),
then batches the L2 coordinate fetches in a single pass.

Output is tab-separated (root_id, x, y, z) ready to paste into Google Sheets.

Usage:
    python fast_get_coords.py --input ids.txt
    python fast_get_coords.py --input ids.txt --output coords.tsv --workers 20
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
    """Parse ID file - handles plain IDs and arrow format."""
    ids = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "\u2192" in line:  # arrow character from validator output
                parts = line.split("\u2192")
                id_part = parts[-1].strip()
                if id_part and id_part.isdigit():
                    ids.append(id_part)
            elif "->" in line:  # ASCII arrow
                parts = line.split("->")
                id_part = parts[-1].strip()
                if id_part and id_part.isdigit():
                    ids.append(id_part)
            elif line.isdigit():
                ids.append(line)
    return ids


def get_one_l2(client, root_id):
    """Get one L2 chunk ID from a root ID. Runs in a thread."""
    try:
        l2_ids = client.chunkedgraph.get_leaves(int(root_id), stop_layer=2)
        if len(l2_ids) > 0:
            return (root_id, l2_ids[0], None)
        return (root_id, None, "No L2 chunks found")
    except Exception as e:
        return (root_id, None, str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Fast parallel coordinate fetcher (ThreadPoolExecutor)"
    )
    parser.add_argument("--input", "-i", required=True, help="Input file with root IDs")
    parser.add_argument("--output", "-o", help="Output TSV file (default: input_coords.tsv)")
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
    print(f"Found {len(all_ids)} IDs to get coordinates for")

    if not all_ids:
        print("No IDs found!", file=sys.stderr)
        sys.exit(1)

    if not args.output:
        input_path = Path(args.input)
        args.output = str(input_path.parent / f"{input_path.stem}_coords.tsv")

    start_time = time.time()

    # Create one shared client
    client = CAVEclient(datastack_name=args.datastack)
    stack_info = client.info.get_datastack_info()
    viewer_res = [
        stack_info["viewer_resolution_x"],
        stack_info["viewer_resolution_y"],
        stack_info["viewer_resolution_z"],
    ]

    # ---- STEP 1: Parallel L2 lookups (THE BOTTLENECK) ----
    print(f"\nStep 1: Getting L2 chunk IDs ({args.workers} parallel workers)...")
    id_to_l2 = {}
    errors = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(get_one_l2, client, rid): rid
            for rid in all_ids
        }
        for future in as_completed(futures):
            root_id, l2_id, err = future.result()
            if l2_id is not None:
                id_to_l2[root_id] = l2_id
            else:
                errors[root_id] = err
            completed += 1
            if completed % 500 == 0 or completed == len(all_ids):
                elapsed = time.time() - start_time
                print(f"  {completed}/{len(all_ids)} L2 IDs fetched ({elapsed:.1f}s)")

    step1_time = time.time() - start_time
    print(f"  Done: {len(id_to_l2)} L2 IDs in {step1_time:.1f}s ({len(errors)} errors)")

    # ---- STEP 2: Batched L2 coordinate fetch ----
    print("\nStep 2: Batch fetching L2 coordinates...")
    step2_start = time.time()

    all_l2_ids = list(id_to_l2.values())
    l2_data = {}
    chunk_size = 100  # L2 cache needs small chunks to avoid 504 timeouts

    for i in range(0, len(all_l2_ids), chunk_size):
        chunk = all_l2_ids[i : i + chunk_size]
        try:
            chunk_data = client.l2cache.get_l2data(chunk, attributes=["rep_coord_nm"])
            l2_data.update(chunk_data)
        except Exception as e:
            print(f"  Warning: L2 chunk {i}-{i + chunk_size} failed: {e}")
        if (i + chunk_size) % 500 == 0 or (i + chunk_size) >= len(all_l2_ids):
            elapsed = time.time() - step2_start
            done = min(i + chunk_size, len(all_l2_ids))
            print(f"  {done}/{len(all_l2_ids)} L2 coords fetched ({elapsed:.1f}s)")

    step2_time = time.time() - step2_start
    print(f"  Done in {step2_time:.1f}s")

    # ---- STEP 3: Build output ----
    print("\nStep 3: Building output...")
    lines = ["root_id\tx\ty\tz"]
    success_count = 0
    fail_count = 0

    for root_id in all_ids:
        if root_id in errors:
            lines.append(f"{root_id}\tERROR\tERROR\tERROR")
            fail_count += 1
            continue

        l2_id = id_to_l2.get(root_id)
        if l2_id is not None:
            l2_key = str(l2_id)
            if l2_key in l2_data and "rep_coord_nm" in l2_data[l2_key]:
                rep_coord = l2_data[l2_key]["rep_coord_nm"]
                x = int(rep_coord[0] / viewer_res[0])
                y = int(rep_coord[1] / viewer_res[1])
                z = int(rep_coord[2] / viewer_res[2])
                lines.append(f"{root_id}\t{x}\t{y}\t{z}")
                success_count += 1
            else:
                lines.append(f"{root_id}\tN/A\tN/A\tN/A")
                fail_count += 1
        else:
            lines.append(f"{root_id}\tN/A\tN/A\tN/A")
            fail_count += 1

    # ---- Write output ----
    output_text = "\n".join(lines)
    with open(args.output, "w") as f:
        f.write(output_text)

    # ---- Summary ----
    total_time = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"COORDINATES COMPLETE - {total_time:.1f} seconds")
    print(f"{'=' * 60}")
    print(f"Total IDs:    {len(all_ids)}")
    print(f"Got coords:   {success_count}")
    print(f"Failed:       {fail_count}")
    print(f"Output:       {args.output}")
    print(f"\nOutput is tab-separated - paste directly into Google Sheets.")


if __name__ == "__main__":
    main()
