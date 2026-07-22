#!/usr/bin/env python3
"""
Update stale BANC root IDs, then check whether each cell is already labeled in
the CAVE `backbone_proofread` table.

Two stages:
  1. ID update -- same supervoxel-tracking method as fast_validate_ids.py
     (get one leaf supervoxel per input root, batch get_roots() at the current
     timestamp). Supervoxels follow physical voxels through splits/merges, so
     this resolves out-of-date roots.
  2. backbone_proofread check -- a materialization live_query at "now" filtered
     to the updated roots. live_query resolves the annotation-side roots to the
     same timestamp, so a match means the cell's current segment carries a
     backbone_proofread annotation right now. We report its `proofread` and
     `valid` flags.

Usage:
    python check_backbone_proofread.py --input ids.txt
    python check_backbone_proofread.py -i ids.txt -o report.tsv --workers 20
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from caveclient import CAVEclient

TABLE_NAME = "backbone_proofread"


def parse_id_file(filepath):
    """Plain IDs or 'N -> ID' / 'N → ID' arrow rows. Preserves order + dups."""
    ids = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            for arrow in ("→", "->"):
                if arrow in line:
                    line = line.split(arrow)[-1].strip()
                    break
            if line.isdigit():
                ids.append(line)
    return ids


def get_one_supervoxel(client, root_id):
    try:
        sv_ids = client.chunkedgraph.get_leaves(int(root_id))
        if len(sv_ids) > 0:
            return (root_id, int(sv_ids[0]), None)
        return (root_id, None, "no supervoxels")
    except Exception as e:
        return (root_id, None, str(e))


def main():
    ap = argparse.ArgumentParser(description="Update BANC IDs + backbone_proofread check")
    ap.add_argument("--input", "-i", required=True)
    ap.add_argument("--output", "-o", help="TSV report (default: <input>_backbone.tsv)")
    ap.add_argument("--datastack", "-d", default="brain_and_nerve_cord")
    ap.add_argument("--workers", "-w", type=int, default=20)
    args = ap.parse_args()

    all_ids = parse_id_file(args.input)
    # de-dup preserving order (report is per unique cell)
    seen = set()
    unique_ids = [x for x in all_ids if not (x in seen or seen.add(x))]
    print(f"Read {len(all_ids)} IDs ({len(unique_ids)} unique)")
    if not unique_ids:
        print("No IDs found!", file=sys.stderr)
        sys.exit(1)

    if not args.output:
        p = Path(args.input)
        args.output = str(p.parent / f"{p.stem}_backbone.tsv")

    t0 = time.time()
    client = CAVEclient(datastack_name=args.datastack)

    # ---- STAGE 1: update roots via supervoxel tracking ----
    print(f"\nStage 1: fetch one supervoxel per ID ({args.workers} workers)...")
    id_to_sv, sv_err = {}, {}
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(get_one_supervoxel, client, rid): rid for rid in unique_ids}
        for fut in as_completed(futs):
            rid, sv, err = fut.result()
            if sv is not None:
                id_to_sv[rid] = sv
            else:
                sv_err[rid] = err
            done += 1
            if done % 50 == 0 or done == len(unique_ids):
                print(f"  {done}/{len(unique_ids)} ({time.time()-t0:.1f}s)")

    sv_list = list(id_to_sv.values())
    sv_to_root = {}
    for i in range(0, len(sv_list), 5000):
        chunk = sv_list[i:i + 5000]
        roots = client.chunkedgraph.get_roots(chunk)
        sv_to_root.update({sv: int(r) for sv, r in zip(chunk, roots)})

    updated = {}          # old_id -> current root (str)
    for rid in unique_ids:
        sv = id_to_sv.get(rid)
        updated[rid] = str(sv_to_root[sv]) if sv in sv_to_root else None
    changed = sum(1 for k, v in updated.items() if v and v != k)
    print(f"  updated: {changed} changed, "
          f"{sum(1 for v in updated.values() if v) - changed} current, "
          f"{len(sv_err)} errored")

    # ---- STAGE 2: backbone_proofread membership via supervoxel mapping ----
    # Supervoxels are immutable, so mapping the table's labeled supervoxels to
    # their CURRENT roots is authoritative regardless of materialization lag.
    # (live_query with a pt_root_id filter is not usable here: it raises on the
    # whole batch if any single filter id is expired vs the mat version.)
    print("\nStage 2: map backbone_proofread supervoxels -> current roots...")
    lab = client.materialize.query_table(
        TABLE_NAME,
        filter_equal_dict={"proofread": True, "valid": True},
        select_columns=["id", "pt_supervoxel_id"],
    )
    svs = lab["pt_supervoxel_id"].astype("int64").unique().tolist()
    labeled_roots = set()
    for i in range(0, len(svs), 100000):
        chunk = svs[i:i + 100000]
        labeled_roots.update(int(r) for r in client.chunkedgraph.get_roots(chunk))
        print(f"  mapped {min(i + 100000, len(svs))}/{len(svs)} supervoxels")
    print(f"  {len(svs)} labeled supervoxels -> "
          f"{len(labeled_roots)} distinct current roots")
    # every stored row is proofread=True, so membership == proofread
    root_status = {r: True for r in labeled_roots}

    # ---- report ----
    rows = []
    for rid in unique_ids:
        new = updated[rid]
        if new is None:
            state, pf = "ERROR", sv_err.get(rid, "lookup failed")
        else:
            nr = int(new)
            if nr in root_status:
                state = "PROOFREAD" if root_status[nr] else "IN_TABLE_NOT_PROOFREAD"
            else:
                state = "NOT_LABELED"
            pf = ""
        rows.append({
            "input_id": rid,
            "current_id": new or "",
            "id_changed": "yes" if (new and new != rid) else "",
            "backbone_status": state,
            "note": pf if state == "ERROR" else "",
        })
    out = pd.DataFrame(rows)
    out.to_csv(args.output, sep="\t", index=False)

    n_pf = (out.backbone_status == "PROOFREAD").sum()
    n_in = (out.backbone_status == "IN_TABLE_NOT_PROOFREAD").sum()
    n_no = (out.backbone_status == "NOT_LABELED").sum()
    n_er = (out.backbone_status == "ERROR").sum()
    print(f"\n{'='*60}\nSUMMARY ({time.time()-t0:.1f}s)\n{'='*60}")
    print(f"Unique cells:            {len(unique_ids)}")
    print(f"IDs updated (changed):   {changed}")
    print(f"backbone_proofread=True: {n_pf}")
    print(f"In table, not proofread: {n_in}")
    print(f"Not in table:            {n_no}")
    print(f"Errors:                  {n_er}")
    print(f"\nReport: {args.output}")
    if n_pf or n_in:
        print("\nCells already in backbone_proofread:")
        for _, r in out[out.backbone_status.isin(["PROOFREAD", "IN_TABLE_NOT_PROOFREAD"])].iterrows():
            tag = "" if r.current_id == r.input_id else f"  (was {r.input_id})"
            print(f"  {r.current_id}  {r.backbone_status}{tag}")


if __name__ == "__main__":
    main()
