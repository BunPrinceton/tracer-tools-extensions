#!/usr/bin/env python3
"""Extract neuroglancer / appspot / CAVE-state links from a Google Sheet.

Scans every cell of one or more worksheets and pulls out anything that looks like
a shareable neuroglancer link or source, so the link pipeline can validate and
restore them in bulk. When a matching cell has an adjacent "Notes" column (a header
containing "note"), that note text is captured alongside the link for context.

Auth reuses the OAuth pattern from tracer_tools' sheets_utils_oauth.py -- YOUR
personal Google account, not a service account. Credentials come from
google_credentials.json (path via --credentials, default: ./google_credentials.json);
the resulting token is cached at ~/.tracer_tools_token.pickle. No secret is printed.

What counts as a link (case-insensitive) -- any URL/source containing:
  appspot.com, json_url=, nglstate, graphene://, ngl.flywire.ai, local_id=, spelunker

Usage:
  python extract_sheet_links.py --sheet <SHEET_ID>
  python extract_sheet_links.py --sheet <SHEET_ID> --worksheet "Sheet1"
  python extract_sheet_links.py --sheet <SHEET_ID> --output links.json
  python extract_sheet_links.py --sheet <SHEET_ID> --output links.csv --format csv
"""
import argparse
import csv
import json
import pickle
import re
import sys
from pathlib import Path

try:
    import gspread
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:  # pragma: no cover
    sys.exit("pip install gspread google-auth-oauthlib")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Keywords that mark a token as a neuroglancer/state link worth extracting.
LINK_KEYWORDS = (
    "appspot.com", "json_url=", "nglstate", "graphene://",
    "ngl.flywire.ai", "local_id=", "spelunker",
)
# Grab URLs (http/https) and bare graphene:// sources out of free text.
TOKEN_RE = re.compile(r"(?:https?://|graphene://)[^\s\"'<>|]+", re.I)


def get_oauth_client(credentials="google_credentials.json"):
    """Authorize gspread with a cached OAuth token (reuses the tracer_tools flow)."""
    creds = None
    token_file = Path.home() / ".tracer_tools_token.pickle"
    if token_file.exists():
        with open(token_file, "rb") as t:
            creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("Opening browser for authorization (one-time)...", file=sys.stderr)
            flow = InstalledAppFlow.from_client_secrets_file(credentials, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "wb") as t:
            pickle.dump(creds, t)
    return gspread.authorize(creds)


def extract_links_from_cell(value):
    """Return the neuroglancer/state links found in one cell's text."""
    if not value:
        return []
    hits = []
    for tok in TOKEN_RE.findall(str(value)):
        tok = tok.rstrip(").,;]}>")  # trailing punctuation from prose
        low = tok.lower()
        if any(k in low for k in LINK_KEYWORDS):
            hits.append(tok)
    return hits


def _notes_columns(rows):
    """Indices of header columns whose name contains 'note' (case-insensitive)."""
    if not rows:
        return []
    header = rows[0]
    return [i for i, h in enumerate(header) if "note" in str(h).strip().lower()]


def _a1(row_idx, col_idx):
    """0-based (row, col) -> A1 notation (e.g. (0,0) -> 'A1')."""
    col = ""
    c = col_idx + 1
    while c:
        c, rem = divmod(c - 1, 26)
        col = chr(65 + rem) + col
    return "%s%d" % (col, row_idx + 1)


def extract_from_worksheet(ws):
    """Yield link records from a single worksheet.

    Each record: {link, sheet, worksheet, cell, row, notes, row_context}.
    """
    rows = ws.get_all_values()
    note_cols = _notes_columns(rows)
    records = []
    for r, row in enumerate(rows):
        note_text = " | ".join(
            row[c].strip() for c in note_cols if c < len(row) and row[c].strip()
        )
        # Compact row context: non-empty, non-link cells (helps a human triage).
        for c, cell in enumerate(row):
            for link in extract_links_from_cell(cell):
                context = " | ".join(
                    x.strip() for j, x in enumerate(row)
                    if j != c and x.strip() and not extract_links_from_cell(x)
                )[:300]
                records.append({
                    "link": link,
                    "worksheet": ws.title,
                    "cell": _a1(r, c),
                    "row": r + 1,
                    "notes": note_text,
                    "row_context": context,
                })
    return records


def main():
    ap = argparse.ArgumentParser(
        description="Extract neuroglancer/appspot/state links from a Google Sheet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Link keywords: " + ", ".join(LINK_KEYWORDS),
    )
    ap.add_argument("--sheet", "-s", required=True, help="Google Sheet ID (from its URL).")
    ap.add_argument("--worksheet", "-w",
                    help="Worksheet/tab name (default: scan every worksheet).")
    ap.add_argument("--credentials", "-c", default="google_credentials.json",
                    help="OAuth client secrets JSON (default: ./google_credentials.json).")
    ap.add_argument("--output", "-o", help="Output path (.json or .csv). Default: stdout JSON.")
    ap.add_argument("--format", choices=["json", "csv"],
                    help="Force output format (else inferred from --output extension).")
    args = ap.parse_args()

    gc = get_oauth_client(args.credentials)
    sh = gc.open_by_key(args.sheet)
    worksheets = ([sh.worksheet(args.worksheet)] if args.worksheet
                  else sh.worksheets())

    records = []
    for ws in worksheets:
        found = extract_from_worksheet(ws)
        records.extend(found)
        print("  %-30s %d links" % (ws.title, len(found)), file=sys.stderr)
    for rec in records:
        rec["sheet"] = args.sheet
    print("Extracted %d links total" % len(records), file=sys.stderr)

    fmt = args.format
    if not fmt and args.output:
        fmt = "csv" if args.output.lower().endswith(".csv") else "json"
    fmt = fmt or "json"

    if fmt == "csv":
        fields = ["link", "sheet", "worksheet", "cell", "row", "notes", "row_context"]
        f = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for rec in records:
            writer.writerow({k: rec.get(k, "") for k in fields})
        if args.output:
            f.close()
    else:
        payload = json.dumps({"count": len(records), "results": records}, indent=2)
        if args.output:
            open(args.output, "w", encoding="utf-8").write(payload)
        else:
            print(payload)

    if args.output:
        print("wrote %s" % args.output, file=sys.stderr)


if __name__ == "__main__":
    main()
