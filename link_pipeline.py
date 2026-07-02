#!/usr/bin/env python3
"""End-to-end link collection -> validation -> restoration pipeline.

Wires together the three stages so you can point it at Google Sheets and/or a text
file of pasted links and get back a single self-contained HTML report of clickable
FIXED links, grouped by liveness status.

Stages:
  1. COLLECT  -- gather links from --sheet <id> (via extract_sheet_links) and/or
                 --input <file> (whitespace / newline / run-together appspot split).
  2. VALIDATE -- classify each with validate_links.classify_link (fetches saved
                 states with your CAVE token when available).
  3. RESTORE  -- for the invalid-but-restorable ones, reuse restore_old_ng_links'
                 fetch() + route() to build the fixed open-in-viewer URL, routing
                 by the state's actual CONTENTS (FlyWire -> ngl.flywire.ai inline,
                 dead MICrONS EM -> repoint + Spelunker, else Spelunker middleauth+).

The HTML writer follows the style of restore_old_ng_links.main (~:152): a grouped
ordered list of "kind -- <id> -- open" rows, self-contained, no external assets.

Auth/secrets are read only from the standard locations (never printed). Sheet
extraction needs google_credentials.json + the one-time OAuth token.

Usage:
  python link_pipeline.py --input links.txt --output report.html
  python link_pipeline.py --sheet <SHEET_ID> --output report.html
  python link_pipeline.py --sheet <ID1> --sheet <ID2> --input more.txt -o report.html
  python link_pipeline.py --input links.txt --no-fetch          # shape-only, offline
  # If restore_old_ng_links.py is not auto-found, pass --restore-path <its dir>.
"""
import argparse
import html
import importlib.util
import json
import os
import sys
import urllib.parse
from pathlib import Path

import validate_links

# Candidate locations of the existing batch restorer (reused, not reinvented).
_RESTORE_CANDIDATES = [
    r"C:\Users\Benjamin\Desktop\Tracer - Workspace\scripts",
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parent.parent / "Tracer - Workspace" / "scripts"),
]

# Statuses whose links we attempt to restore (the rest are shown as-is / unfixable).
RESTORABLE = {"dead-host", "auth-gated", "dead-em", "seg-gone"}


def load_restore_module(extra_path=None):
    """Import restore_old_ng_links.py from a known location. Returns the module or None."""
    paths = ([extra_path] if extra_path else []) + _RESTORE_CANDIDATES
    for d in paths:
        if not d:
            continue
        f = os.path.join(d, "restore_old_ng_links.py")
        if os.path.exists(f):
            spec = importlib.util.spec_from_file_location("restore_old_ng_links", f)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    return None


def collect_from_file(path):
    """Read a text file and split it into individual links."""
    text = open(path, encoding="utf-8").read()
    return [{"link": lk, "source": os.path.basename(path)}
            for lk in validate_links.split_links(text)]


def collect_from_sheet(sheet_id, credentials, worksheet=None):
    """Extract links from a Google Sheet (lazy import -- only needed with --sheet)."""
    import extract_sheet_links as esl
    gc = esl.get_oauth_client(credentials)
    sh = gc.open_by_key(sheet_id)
    worksheets = [sh.worksheet(worksheet)] if worksheet else sh.worksheets()
    items = []
    for ws in worksheets:
        for rec in esl.extract_from_worksheet(ws):
            items.append({
                "link": rec["link"],
                "source": "sheet:%s!%s" % (sheet_id[:8], rec["cell"]),
                "notes": rec.get("notes", ""),
            })
    return items


def restore_one(item, restore_mod, tokens):
    """Try to build a fixed open URL for one validated item. Returns (kind, url|None)."""
    cls = item["classification"]
    link = item["link"]
    if restore_mod is None:
        return "restore module unavailable", None
    try:
        # Self-contained inline/raw-JSON states carry the whole scene: route() can
        # swap a dead EM / add middleauth without any fetch (srv/sid unused there).
        if cls["shape"] in ("inline", "json"):
            state = validate_links._inline_json(link)
            if not isinstance(state, dict):
                return "inline state unparseable", None
            if cls["status"] == "ok":
                return "self-contained inline state", link
            return restore_mod.route(None, None, state)

        sid = cls.get("state_id")
        if not sid:
            return "no state id to restore", None
        srv, state = restore_mod.fetch(cls.get("host"), sid, tokens)
        # fetch() returns (None, <status-int|err-str>) on failure -- guard on dict.
        if not isinstance(state, dict):
            return "fetch failed (%s)" % state, None
        return restore_mod.route(srv, sid, state)
    except Exception as e:  # noqa: BLE001
        return "restore error: " + str(e)[:60], None


def write_report(items, out_path):
    """Write a single self-contained HTML report, grouped by status."""
    groups = {}
    for it in items:
        groups.setdefault(it["classification"]["status"], []).append(it)

    order = list(validate_links.STATUSES)
    sections = []
    for st in order:
        bucket = groups.get(st)
        if not bucket:
            continue
        rows = []
        for it in bucket:
            cls = it["classification"]
            sid = cls.get("state_id") or "-"
            kind = it.get("restore_kind", "")
            url = it.get("restore_url")
            open_a = (' &mdash; <a href="%s">open</a>'
                      % html.escape(url, quote=True)) if url else ""
            note = it.get("notes", "")
            note_html = (' <span style="color:#999">(%s)</span>'
                         % html.escape(note[:80])) if note else ""
            detail = html.escape(cls.get("detail", ""))
            rows.append(
                '<li style="margin:.45rem 0">%s &mdash; <code>%s</code> '
                '<span style="color:#666">%s</span>%s%s'
                '<div style="color:#888;font-size:.8rem;margin-left:1rem">%s</div></li>'
                % (html.escape(kind or cls["shape"]), html.escape(sid),
                   html.escape(it["source"]), open_a, note_html, detail))
        sections.append(
            '<h3 style="margin-top:1.6rem">%s <span style="color:#999;font-weight:400">'
            '(%d)</span></h3><ol>%s</ol>' % (html.escape(st), len(bucket), "".join(rows)))

    doc = (
        '<!DOCTYPE html><meta charset="utf-8"><title>link pipeline report</title>'
        '<body style="font:15px system-ui;margin:2rem;max-width:960px">'
        '<h2>Link pipeline report &mdash; %d links</h2>'
        '<p style="color:#666;font-size:.85rem">Grouped by liveness status. '
        'FlyWire links open in ngl.flywire.ai (log into FlyWire); MICrONS links open '
        'in Spelunker (log into CAVE). Generated by link_pipeline.py.</p>%s</body>'
        % (len(items), "".join(sections)))
    open(out_path, "w", encoding="utf-8").write(doc)


def main():
    ap = argparse.ArgumentParser(
        description="Collect -> validate -> restore neuroglancer links into one HTML report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Restorable statuses: " + ", ".join(sorted(RESTORABLE)),
    )
    ap.add_argument("--sheet", "-s", action="append", default=[],
                    help="Google Sheet ID (repeatable).")
    ap.add_argument("--worksheet", "-w", help="Worksheet name (applies to all --sheet).")
    ap.add_argument("--input", "-i", action="append", default=[],
                    help="Text file of links (repeatable).")
    ap.add_argument("--credentials", "-c", default="google_credentials.json",
                    help="OAuth client secrets JSON for --sheet.")
    ap.add_argument("--output", "-o", default="link_report.html",
                    help="Output HTML report (default: link_report.html).")
    ap.add_argument("--restore-path", help="Directory containing restore_old_ng_links.py.")
    ap.add_argument("--no-fetch", action="store_true",
                    help="Shape-only validation and no restoration fetches (offline).")
    ap.add_argument("--json-out", help="Also dump the raw records as JSON here.")
    args = ap.parse_args()

    if not args.sheet and not args.input:
        ap.error("provide at least one --sheet or --input")

    # ---- 1. COLLECT ----
    items = []
    for path in args.input:
        items.extend(collect_from_file(path))
    for sid in args.sheet:
        items.extend(collect_from_sheet(sid, args.credentials, args.worksheet))
    print("collected %d links" % len(items))
    if not items:
        sys.exit("no links found")

    # ---- 2. VALIDATE ----
    tokens = [] if args.no_fetch else validate_links.load_tokens()
    if not args.no_fetch and not tokens:
        print("No CAVE token found -- validating/restoring shape-only.", file=sys.stderr)
    for it in items:
        it["classification"] = validate_links.classify_link(
            it["link"], tokens=tokens, do_fetch=not args.no_fetch)

    # ---- 3. RESTORE ----
    # Load unconditionally: route() restores inline states with no network, so it
    # is useful even under --no-fetch.
    restore_mod = load_restore_module(args.restore_path)
    if restore_mod is None:
        print("restore_old_ng_links.py not found -- reporting validation only "
              "(pass --restore-path).", file=sys.stderr)
    for it in items:
        cls = it["classification"]
        st, shape = cls["status"], cls["shape"]
        inline = shape in ("inline", "json")
        # Inline states restore with no network; fetch-based ones need a token.
        if restore_mod is not None and (inline or (st in RESTORABLE and tokens)):
            it["restore_kind"], it["restore_url"] = restore_one(it, restore_mod, tokens)

    # ---- report ----
    write_report(items, args.output)
    counts = {}
    for it in items:
        counts[it["classification"]["status"]] = \
            counts.get(it["classification"]["status"], 0) + 1
    print("\nSummary:")
    for st in validate_links.STATUSES:
        if counts.get(st):
            fixed = sum(1 for it in items
                        if it["classification"]["status"] == st and it.get("restore_url"))
            print("  %-20s %d  (%d fixed)" % (st, counts[st], fixed))
    print("\nwrote %s" % args.output)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)
        print("wrote %s" % args.json_out)


if __name__ == "__main__":
    main()
