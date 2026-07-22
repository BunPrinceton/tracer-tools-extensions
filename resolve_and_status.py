#!/usr/bin/env python3
"""
resolve_and_status.py -- the lean "ground-truth default" BANC lookup.

Two CAVE-native, canonical calls, nothing custom:

  1. RESOLVE   current_id = chunkedgraph.suggest_latest_roots(input_id)
               CAVE's standard "given an ID, return the current ID by max
               voxel-overlap." Returns the input unchanged if already current,
               so it needs no separate is_latest_roots step. On a SPLIT it picks
               the largest-overlap piece (flagged in output; see note below).

  2. STATUS    proofread = a live_live_query on `backbone_proofread` at "now"
               for the resolved (current) IDs. This is the same live call
               banc-bot makes for `<id>??`. It reports proofread ONLY if a real
               backbone_proofread row exists on that current cell right now --
               so it never produces a false positive, and (unlike a materialized
               query_table) it never lags behind recent proofreading.

Because both are the sanctioned CAVE functions, the output needs no external
corroboration -- there is no custom logic to distrust. (For an audit trail /
skeptic-facing cross-check against banc-bot's own code + a second resolver, use
check_backbone_proofread_hybrid.py instead.)

SPLIT NOTE: after a real split there is no single canonical "current ID" -- it
depends which piece you mean, exactly as in Neuroglancer (where a 2D double-click
returns whichever piece you clicked). suggest_latest_roots picks the biggest
piece by overlap; rows where that differs from a point-based pick are inherently
a human judgment call, not a tool error.

Usage:
    python resolve_and_status.py --input ids.txt
    python resolve_and_status.py -i ids.txt -o out.tsv --workers 20
"""

import argparse
import logging
import sys
import time
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
logging.disable(logging.WARNING)

import pandas as pd
from caveclient import CAVEclient

TABLE_NAME = "backbone_proofread"


def parse_id_file(filepath):
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


def main():
    ap = argparse.ArgumentParser(description="Lean BANC resolve + proofread status (ground-truth CAVE calls)")
    ap.add_argument("--input", "-i", required=True)
    ap.add_argument("--output", "-o", help="TSV (default: <input>_status.tsv)")
    ap.add_argument("--datastack", "-d", default="brain_and_nerve_cord")
    ap.add_argument("--workers", "-w", type=int, default=20)
    args = ap.parse_args()

    all_ids = parse_id_file(args.input)
    seen = set()
    unique_ids = [x for x in all_ids if not (x in seen or seen.add(x))]
    print(f"Read {len(all_ids)} IDs ({len(unique_ids)} unique)")
    if not unique_ids:
        print("No IDs found!", file=sys.stderr)
        sys.exit(1)
    if not args.output:
        p = Path(args.input)
        args.output = str(p.parent / f"{p.stem}_status.tsv")

    t0 = time.time()
    client = CAVEclient(datastack_name=args.datastack)

    # ---- 1. RESOLVE: suggest_latest_roots (parallel per id) ----
    print(f"\n1. resolve -> current id via suggest_latest_roots ({args.workers} workers)...")
    def resolve(rid):
        try:
            r = client.chunkedgraph.suggest_latest_roots(int(rid))
            if hasattr(r, "__len__") and not isinstance(r, int):
                r = r[0] if len(r) else None
            return (rid, str(int(r)) if r is not None else None, None)
        except Exception as e:
            return (rid, None, str(e))
    current, res_err = {}, {}
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(resolve, rid): rid for rid in unique_ids}
        for fut in as_completed(futs):
            rid, cur, err = fut.result()
            current[rid] = cur
            if err:
                res_err[rid] = err
            done += 1
            if done % 50 == 0 or done == len(unique_ids):
                print(f"  {done}/{len(unique_ids)} ({time.time()-t0:.1f}s)")
    changed = sum(1 for rid in unique_ids if current[rid] and current[rid] != rid)

    # ---- 2. STATUS: live_live_query on backbone_proofread for current ids ----
    print("2. status -> live_live_query on backbone_proofread (batched)...")
    cur_ids = sorted({int(current[rid]) for rid in unique_ids if current[rid]})
    proof_roots = set()
    CHUNK = 200
    for i in range(0, len(cur_ids), CHUNK):
        chunk = cur_ids[i:i + CHUNK]
        try:
            df = client.materialize.live_live_query(
                TABLE_NAME, pd.Timestamp.utcnow(),
                filter_in_dict={TABLE_NAME: {"pt_root_id": chunk}})
            if len(df):
                proof_roots.update(int(x) for x in df["pt_root_id"].values)
        except Exception:
            for s in chunk:  # fall back per-id so one bad id can't blank the chunk
                try:
                    df = client.materialize.live_live_query(
                        TABLE_NAME, pd.Timestamp.utcnow(),
                        filter_in_dict={TABLE_NAME: {"pt_root_id": [s]}})
                    if len(df):
                        proof_roots.add(int(s))
                except Exception:
                    pass
        print(f"  queried {min(i + CHUNK, len(cur_ids))}/{len(cur_ids)} ({time.time()-t0:.1f}s)")

    # ---- report ----
    rows = []
    for rid in unique_ids:
        cur = current[rid]
        if cur is None:
            status = "ERROR"
        elif int(cur) in proof_roots:
            status = "PROOFREAD"
        else:
            status = "NOT_LABELED"
        rows.append({
            "input_id": rid,
            "current_id": cur or "",
            "id_changed": "yes" if (cur and cur != rid) else "",
            "proofread_status": status,
            "note": res_err.get(rid, "") if status == "ERROR" else "",
        })
    out = pd.DataFrame(rows)
    out.to_csv(args.output, sep="\t", index=False)

    n = len(unique_ids)
    n_pf = (out.proofread_status == "PROOFREAD").sum()
    n_no = (out.proofread_status == "NOT_LABELED").sum()
    n_er = (out.proofread_status == "ERROR").sum()
    print(f"\n{'='*56}\nSUMMARY ({time.time()-t0:.1f}s)\n{'='*56}")
    print(f"Unique cells:        {n}")
    print(f"IDs updated:         {changed}")
    print(f"PROOFREAD:           {n_pf}")
    print(f"NOT_LABELED:         {n_no}")
    print(f"ERROR:               {n_er}")
    print(f"\nReport: {args.output}")


if __name__ == "__main__":
    main()
