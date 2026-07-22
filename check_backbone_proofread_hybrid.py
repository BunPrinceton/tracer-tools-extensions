#!/usr/bin/env python3
"""
HYBRID / CORROBORATED version of check_backbone_proofread.py.

This does NOT replace check_backbone_proofread.py -- that tool is unchanged and
still the primary. This one runs the SAME IDs through two engines side by side
and reports where they agree/disagree, so results carry banc-bot's stamp:

  ENGINE A  "ours"      -- the exact method from check_backbone_proofread.py:
                          supervoxel tracking (get_leaves -> get_roots) to update
                          stale roots, and a backbone_proofread table map
                          (query_table + get_roots) for proofread status.

  ENGINE B  "banc-bot"  -- banc-bot's OWN code, imported from the `banc` package
                          (banc/ is a symlink to Jasper Phelps' `fanc` package;
                          it is literally what @banc-bot runs):
                            * caveclient.chunkedgraph.is_latest_roots()  -> currency
                            * banc.lookup.annotations(id, return_details=True)
                              (the `<id>??` command) -> proofread status + the
                              verbatim reply banc-bot would post in Slack.
                          banc-bot cannot resolve a stale ID (it just rejects it
                          with an ERROR), so for the 58-style stale cases Engine B
                          confirms the OLD id is stale and confirms our resolved
                          NEW id is current + reports its status.

  ENGINE C  "cave"      -- request #3: an independent, source-pointable ID
                          resolver. The `banc`/`fanc` package has NO outdated-ID
                          resolver (proofreading_status/anchor_point/soma_from_segid
                          all just raise "Use updated IDs"), so the trusted
                          reference is caveclient itself:
                            caveclient.chunkedgraph.suggest_latest_roots()
                          -- CAVE's canonical "given an old root, pick the current
                          root by max L2 overlap." We compare it to our supervoxel
                          method's answer.

Output: one row per unique input ID, ours + banc-bot + cave columns side by side,
plus agreement flags and a hybrid verdict. Optionally writes a `*_bancbot_log.txt`
"offline chat log" of the exact `<id>??` exchanges (audit trail).

Usage:
    python check_backbone_proofread_hybrid.py --input ids.txt
    python check_backbone_proofread_hybrid.py -i ids.txt -o report.tsv --workers 20
    python check_backbone_proofread_hybrid.py -i ids.txt --sample 30      # spot-check subset
    python check_backbone_proofread_hybrid.py -i ids.txt --no-banc-log    # skip the chat-log sidecar
"""

import argparse
import logging
import sys
import time
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
logging.disable(logging.WARNING)  # silence banc's beta/live_query deprecation spam

import pandas as pd
from caveclient import CAVEclient

TABLE_NAME = "backbone_proofread"

# banc-bot's exact stale-ID reply (annotation_bot.py), reproduced for the log.
BANC_STALE_ERR = ("ERROR: {segid} is not a current segment ID."
                  " It may have been edited recently, or perhaps"
                  " you copy-pasted the wrong thing.")


def parse_id_file(filepath):
    """Plain IDs or 'N -> ID' / 'N -> ID' arrow rows. Preserves order + dups."""
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


def setup_banc():
    """Import banc and point it at the working default token key.

    banc.auth defaults to auth_token_key='brain_and_nerve_cord', but caveclient
    reads our token from the default 'token' key -- so we redirect banc to it.
    Returns the banc.lookup module, or None if banc isn't importable.
    """
    try:
        import banc
        from banc import auth, lookup
        auth.configs["cave_auth_token_key"] = "token"
        for cache in ("_clients", "_cloudvolumes"):
            if hasattr(auth, cache):
                getattr(auth, cache).clear()
        return lookup
    except Exception as e:
        print(f"WARNING: could not import banc ({type(e).__name__}: {e}).\n"
              "         Install with `pip install banc`. Falling back to a "
              "faithful replica of banc-bot's live_live_query call.", file=sys.stderr)
        return None


def banc_annotations_replica(client, segids, tables=("backbone_proofread",
                                                     "cell_info",
                                                     "proofreading_notes")):
    """Fallback if `banc` isn't installed: banc.lookup.annotations' exact call
    (materialize.live_live_query, filter_in_dict on pt_root_id), same tables.
    Accepts a list of segids so it batches like the real banc.lookup does."""
    ids = [int(s) for s in segids]
    frames = []
    for t in tables:
        try:
            df = client.materialize.live_live_query(
                t, pd.Timestamp.utcnow(),
                filter_in_dict={t: {"pt_root_id": ids}},
                allow_missing_lookups=True)
            if len(df):
                frames.append(df.assign(source_table=t))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["pt_root_id", "source_table"])
    return pd.concat(frames, ignore_index=True)


def render_banc_reply(df):
    """Reproduce banc-bot's `<id>??` message body from an annotations dataframe."""
    d = df.copy()
    d.drop(columns=["id", "valid", "pt_supervoxel_id", "pt_root_id",
                    "pt_position", "deleted", "superceded_id"],
           errors="ignore", inplace=True)
    d.rename(columns={"tag": "annotation", "tag2": "annotation_class"},
             inplace=True)
    if "created" in d.columns:
        try:
            d["created"] = d["created"].apply(lambda x: x.date())
        except Exception:
            pass
    return "```\n" + d.to_string(index=False) + "\n```"


def main():
    ap = argparse.ArgumentParser(description="Hybrid BANC ID/proofread check (ours + banc-bot + cave)")
    ap.add_argument("--input", "-i", required=True)
    ap.add_argument("--output", "-o", help="TSV report (default: <input>_hybrid.tsv)")
    ap.add_argument("--datastack", "-d", default="brain_and_nerve_cord")
    ap.add_argument("--workers", "-w", type=int, default=20)
    ap.add_argument("--sample", type=int, default=0,
                    help="only run the first N unique IDs (spot-check to spare banc-bot's backend)")
    ap.add_argument("--no-banc-log", action="store_true",
                    help="skip the *_bancbot_log.txt offline chat-log sidecar")
    args = ap.parse_args()

    all_ids = parse_id_file(args.input)
    seen = set()
    unique_ids = [x for x in all_ids if not (x in seen or seen.add(x))]
    if args.sample and args.sample < len(unique_ids):
        unique_ids = unique_ids[:args.sample]
        print(f"SAMPLE mode: first {len(unique_ids)} unique IDs")
    print(f"Read {len(all_ids)} IDs ({len(unique_ids)} to check)")
    if not unique_ids:
        print("No IDs found!", file=sys.stderr)
        sys.exit(1)

    if not args.output:
        p = Path(args.input)
        args.output = str(p.parent / f"{p.stem}_hybrid.tsv")

    t0 = time.time()
    client = CAVEclient(datastack_name=args.datastack)
    lookup = setup_banc()

    # =========================================================================
    # ENGINE A (ours) -- identical logic to check_backbone_proofread.py
    # =========================================================================
    print(f"\n[A/ours] Stage 1: supervoxel-track each ID -> current root ({args.workers} workers)...")
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

    our_current = {}  # old_id -> current root (str) or None
    for rid in unique_ids:
        sv = id_to_sv.get(rid)
        our_current[rid] = str(sv_to_root[sv]) if sv in sv_to_root else None
    changed = sum(1 for k, v in our_current.items() if v and v != k)
    print(f"  ours: {changed} changed, "
          f"{sum(1 for v in our_current.values() if v) - changed} current, "
          f"{len(sv_err)} errored")

    print(f"[A/ours] Stage 2: map {TABLE_NAME} supervoxels -> current roots...")
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

    def our_pf(rid):
        cur = our_current[rid]
        if cur is None:
            return "ERROR"
        return "PROOFREAD" if int(cur) in labeled_roots else "NOT_LABELED"

    # =========================================================================
    # ENGINE B (banc-bot) + ENGINE C (cave resolver)
    # =========================================================================
    print("\n[B/banc-bot] currency via is_latest_roots (batched)...")
    input_ints = [int(x) for x in unique_ids]
    input_latest = dict(zip(unique_ids,
                            [bool(b) for b in client.chunkedgraph.is_latest_roots(input_ints)]))

    print("[C/cave] resolve via suggest_latest_roots (request #3, parallel)...")
    def suggest(rid):
        try:
            r = client.chunkedgraph.suggest_latest_roots(int(rid))
            if hasattr(r, "__len__") and not isinstance(r, (int,)):
                r = r[0] if len(r) else None
            return (rid, str(int(r)) if r is not None else None, None)
        except Exception as e:
            return (rid, None, str(e))
    cave_suggested = {}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(suggest, rid) for rid in unique_ids]):
            rid, sug, err = fut.result()
            cave_suggested[rid] = sug

    print("[B/banc-bot] proofread status via banc.lookup annotations (the `??` command, batched)...")

    def fetch_annotations(ids_int):
        if lookup is not None:
            return lookup.annotations(ids_int, return_details=True)
        return banc_annotations_replica(client, ids_int)

    # Query banc for the CURRENT id of each cell (banc-bot only answers on
    # current ids). banc.lookup.annotations takes a list, so batch it; if a
    # chunk raises (e.g. one id unexpectedly not-latest), fall back per-id so a
    # single bad id can't blank the whole chunk.
    # Include BOTH our resolved id AND cave's suggested id, so on split /
    # resolver-conflict rows we can report the cave pick's proofread status too
    # (otherwise a proofread on the "other" split fragment stays hidden).
    query_ids = set()
    for rid in unique_ids:
        if our_current[rid]:
            query_ids.add(int(our_current[rid]))
        if cave_suggested.get(rid):
            query_ids.add(int(cave_suggested[rid]))
    current_ids = sorted(query_ids)
    CHUNK = 250
    frames = []
    for i in range(0, len(current_ids), CHUNK):
        chunk = current_ids[i:i + CHUNK]
        try:
            df = fetch_annotations(chunk)
            if len(df):
                frames.append(df)
        except Exception:
            for s in chunk:
                try:
                    df = fetch_annotations([s])
                    if len(df):
                        frames.append(df)
                except Exception:
                    pass
        print(f"  queried {min(i + CHUNK, len(current_ids))}/{len(current_ids)} "
              f"current ids ({time.time()-t0:.1f}s)")

    banc_all = (pd.concat(frames, ignore_index=True) if frames
                else pd.DataFrame(columns=["pt_root_id", "source_table"]))
    if len(banc_all):
        banc_all["pt_root_id"] = banc_all["pt_root_id"].astype("int64")
    groups = {int(k): v for k, v in banc_all.groupby("pt_root_id")} if len(banc_all) else {}

    def banc_pf_for(root):
        """Proofread verdict from banc's live annotations for one current root."""
        if root is None:
            return "ERROR"
        g = groups.get(int(root))
        if g is None:
            return "NOT_LABELED"  # current id, but no annotations at all
        src = g["source_table"].astype(str).values
        return "PROOFREAD" if any(s == TABLE_NAME for s in src) else "NOT_LABELED"

    banc_pf, banc_df, banc_pf_cave = {}, {}, {}
    for rid in unique_ids:
        cur = our_current[rid]
        sug = cave_suggested.get(rid)
        banc_pf[rid] = banc_pf_for(int(cur)) if cur else "ERROR"
        banc_df[rid] = groups.get(int(cur)) if cur else None
        # banc verdict on cave's suggested id (== banc_pf when they match)
        banc_pf_cave[rid] = banc_pf_for(int(sug)) if sug else ""

    # =========================================================================
    # Compare + hybrid verdict
    # =========================================================================
    rows = []
    for rid in unique_ids:
        cur = our_current[rid]
        our_changed = bool(cur and cur != rid)
        banc_stale = (input_latest.get(rid) is False)
        opf = our_pf(rid)
        bpf = banc_pf[rid]
        sug = cave_suggested.get(rid)

        # currency: ours says "changed" iff banc says "not latest"
        currency_agree = (our_changed == banc_stale) if cur is not None else None
        # resolver (request #3): our supervoxel result vs cave's suggest_latest_roots
        if cur is None or sug is None:
            resolver_agree = None
        else:
            resolver_agree = (cur == sug)
        cpf = banc_pf_cave[rid]  # banc verdict on cave's suggested id
        # proofread: both engines on the current cell
        if opf.startswith("ERROR") or bpf.startswith("ERROR"):
            proofread_agree = None
        else:
            proofread_agree = (opf == bpf)

        disagree = []
        if currency_agree is False:
            disagree.append("currency")
        if proofread_agree is False:
            disagree.append("proofread")
        if resolver_agree is False:
            disagree.append("resolver")

        # ---- reconciled "best truth" ----
        # ID: prefer cave's suggest_latest_roots (max-overlap; robust on splits);
        #     fall back to ours if cave couldn't resolve.
        best_id = sug or cur or ""
        # Proofread: live answer wins over the materialized snapshot. If ours and
        # cave disagree on the ID, a PROOFREAD on EITHER fragment means proofread
        # exists on this lineage -> report PROOFREAD (and flag the split).
        live_verdicts = [v for v in (bpf, cpf) if v in ("PROOFREAD", "NOT_LABELED")]
        if not live_verdicts:
            best_pf = opf  # banc errored; best we have is ours
        elif "PROOFREAD" in live_verdicts:
            best_pf = "PROOFREAD"
        else:
            best_pf = "NOT_LABELED"

        if opf.startswith("ERROR") or bpf.startswith("ERROR"):
            status = "ERROR"
        elif disagree:
            status = "CHECK:" + "+".join(disagree)
        else:
            status = "VERIFIED"

        rows.append({
            "input_id": rid,
            "our_current_id": cur or "",
            "cave_suggested_id": sug or "",
            "resolver_agree": {True: "yes", False: "NO", None: ""}[resolver_agree],
            "our_id_changed": "yes" if our_changed else "",
            "banc_input_is_latest": {True: "yes", False: "no", None: ""}[input_latest.get(rid)],
            "currency_agree": {True: "yes", False: "NO", None: ""}[currency_agree],
            "our_proofread": opf,
            "banc_proofread": bpf,
            "cave_pick_proofread": cpf,
            "proofread_agree": {True: "yes", False: "NO", None: ""}[proofread_agree],
            "hybrid_status": status,
            "best_current_id": best_id,
            "best_proofread": best_pf,
        })

    out = pd.DataFrame(rows)
    out.to_csv(args.output, sep="\t", index=False)

    # ---- offline banc-bot "chat log" sidecar ----
    if not args.no_banc_log:
        log_path = str(Path(args.output).with_name(Path(args.output).stem + "_bancbot_log.txt"))
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("# Offline reproduction of banc-bot `<id>??` exchanges\n")
            f.write("# (generated from banc.lookup -- banc-bot's own code)\n\n")
            for rid in unique_ids:
                cur = our_current[rid]
                sug = cave_suggested.get(rid)
                if input_latest.get(rid) is False:
                    # banc-bot rejects the stale input, then we ask the current id
                    f.write(f"{rid}??\n{BANC_STALE_ERR.format(segid=rid)}\n")
                    if cur:
                        f.write(f"{cur}??   (current ID resolved from {rid})\n")
                        df = banc_df.get(rid)
                        f.write((render_banc_reply(df) if df is not None and len(df)
                                 else "No annotations found.") + "\n\n")
                    else:
                        f.write("\n")
                else:
                    f.write(f"{rid}??\n")
                    df = banc_df.get(rid)
                    f.write((render_banc_reply(df) if df is not None and len(df)
                             else "No annotations found.") + "\n\n")
                # on a resolver split, also show the cave-suggested fragment
                if sug and cur and sug != cur:
                    f.write(f"{sug}??   (cave suggest_latest_roots pick for {rid}, "
                            f"differs from ours {cur})\n")
                    dfg = groups.get(int(sug))
                    f.write((render_banc_reply(dfg) if dfg is not None and len(dfg)
                             else "No annotations found.") + "\n\n")
        print(f"banc-bot chat log: {log_path}")

    # ---- summary ----
    n = len(unique_ids)
    n_ver = (out.hybrid_status == "VERIFIED").sum()
    n_chk = out.hybrid_status.str.startswith("CHECK").sum()
    n_err = (out.hybrid_status == "ERROR").sum()
    pf_agree = (out.proofread_agree == "yes").sum()
    pf_conf = (out.proofread_agree == "NO").sum()
    cur_conf = (out.currency_agree == "NO").sum()
    res_conf = (out.resolver_agree == "NO").sum()
    our_pf_n = (out.our_proofread == "PROOFREAD").sum()
    best_pf_n = (out.best_proofread == "PROOFREAD").sum()
    print(f"\n{'='*64}\nHYBRID SUMMARY ({time.time()-t0:.1f}s)\n{'='*64}")
    print(f"Unique cells checked:            {n}")
    print(f"  VERIFIED (all engines agree):  {n_ver}")
    print(f"  CHECK (some disagreement):     {n_chk}")
    print(f"  ERROR:                         {n_err}")
    print(f"Proofread status agree (ours==banc-bot): {pf_agree}/{n}  (conflicts: {pf_conf})")
    print(f"Currency agree (ours==banc-bot is_latest): conflicts: {cur_conf}")
    print(f"Resolver agree (ours==cave suggest_latest): conflicts: {res_conf}")
    print(f"RECONCILED proofread (best_proofread): {best_pf_n}   "
          f"(our tool alone: {our_pf_n}, +{best_pf_n - our_pf_n})")
    print(f"\nReport:  {args.output}")
    if n_chk or n_err:
        print("\nRows needing a look (reconciled truth in best_* columns):")
        for _, r in out[~(out.hybrid_status == "VERIFIED")].iterrows():
            print(f"  {r.input_id}  {r.hybrid_status:16s} "
                  f"ours={r.our_proofread:11s} banc={r.banc_proofread:11s} "
                  f"cave_pick={str(r.cave_pick_proofread):11s} "
                  f"-> best={r.best_proofread} @ {r.best_current_id}")


if __name__ == "__main__":
    main()
