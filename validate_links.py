#!/usr/bin/env python3
"""Validate the liveness of old neuroglancer / appspot / CAVE-state share links.

Companion to restore_old_ng_links.py (the batch restorer) and the borkbook Link
Restorer web tool (/link-restore/). Where the restorer *fixes* links, this module
just *classifies* them: given any link shape, decide whether it still works and, if
not, why. That lets a pipeline triage a big pile of links before spending effort on
restoration.

Shape detection is ported from the web tool's JS restore() (link-restore/index.html
~:565) so it covers the same zoo of link shapes:
  - browser-local `?local_id=<hex>` scenes (NOT portable / not restorable)
  - inline `#!{json}` self-contained states
  - short `?json_url=.../nglstate/<id>` stored-state links
  - bare `nglstate` URLs and bare numeric state ids
  - raw `graphene://` sources
  - retired-host appspot / dynamicannotationframework.com links
  - a truncated-id heuristic (real CAVE state ids are ~16 digits)

For links that carry a stored-state id we optionally FETCH the saved state with a
CAVE Bearer token (same secrets + dual-server dance as restore_old_ng_links.fetch)
and route by the state's actual CONTENTS -- because, as that script's key lesson
says, a link's HOST does not identify the dataset; only the fetched contents do.

STATUSES (classify_link returns exactly one):
  ok                  -- link is live / fetchable / self-contained and loads
  dead-host           -- link's host is retired and we could not confirm a live state
  truncated-id        -- state id is too short (link was cut off in transit)
  auth-gated          -- requires a CAVE login we could not complete here
  dead-em             -- state loads but its EM image was garbage-collected (repointable)
  seg-gone            -- state loads but its segmentation source is deprecated/dead
  local-id-unportable -- ?local_id= scene; lives only in the browser that made it
  unrecognized        -- no neuroglancer/state link recognized in the input

Auth: reads your token(s) from ~/.cloudvolume/secrets/*cave-secret.json (never
printed). Get one with:
  python -c "from caveclient import CAVEclient; print(CAVEclient().auth.token)"

Usage:
  python validate_links.py --input links.txt
  python validate_links.py --input links.txt --output report.json
  python validate_links.py --input links.txt --no-fetch      # shape-only, no network
  # links.txt is one link per line, or links concatenated with no separators.
"""
import argparse
import json
import os
import re
import sys
import urllib.parse

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("pip install requests  (it ships with caveclient)")

SECRETS = [
    os.path.expanduser("~/.cloudvolume/secrets/cave-secret.json"),
    os.path.expanduser("~/.cloudvolume/secrets/global.daf-apis.com-cave-secret.json"),
]
STATE_SERVERS = [
    "https://global.daf-apis.com/nglstate/api/v1/",
    "https://globalv1.flywire-daf.com/nglstate/api/v1/",
]

# Statuses this module can assign (documented in the module docstring).
STATUSES = (
    "ok", "dead-host", "truncated-id", "auth-gated",
    "dead-em", "seg-gone", "local-id-unportable", "unrecognized",
)

RETIRED_HOST_RE = re.compile(
    r"dynamicannotationframework\.com|neuromancer-seung-import\.appspot\.com",
    re.I,
)
# A real CAVE/Datastore state id is ~16 digits; much shorter almost always means
# the pasted link got truncated (long URLs get cut in chat / docs / sheets).
MIN_STATE_ID_DIGITS = 15


def load_tokens():
    """Return CAVE bearer tokens from the standard secret files (never printed)."""
    toks = []
    for p in SECRETS:
        try:
            t = json.load(open(p)).get("token", "")
            if t and t not in toks:
                toks.append(t)
        except Exception:
            pass
    return toks


# Keyword/shape markers that make a whitespace token worth treating as a link.
_LINK_MARKERS = re.compile(
    r"://|graphene:|json_url=|nglstate|local_id=|appspot\.com|"
    r"ngl\.flywire\.ai|spelunker",
    re.I,
)


def split_links(text):
    """Split a blob into individual link-like tokens.

    Handles one-per-line, whitespace-separated, AND appspot links concatenated
    with no separator (the old share-doc failure mode). Broader than
    restore_old_ng_links.split_links (which only kept appspot json_url links) but
    filtered so surrounding prose does not fragment into noise: a token is kept
    only if it carries a link marker or is a bare numeric state id.
    """
    # First break apart run-together appspot links (no separator between them).
    text = re.sub(
        r"(?=https?://neuromancer-seung-import\.appspot\.com)", "\n", text
    )
    out = []
    for tok in re.split(r"\s+", text):
        tok = tok.strip()
        if not tok:
            continue
        if _LINK_MARKERS.search(tok) or re.fullmatch(r"\d{6,}", tok):
            out.append(tok)
    return out


def _srcof(layer):
    """Flatten a neuroglancer layer's source (str | dict | list) to a string."""
    s = layer.get("source")
    if isinstance(s, dict):
        s = s.get("url")
    if isinstance(s, list):
        s = ",".join(x.get("url") if isinstance(x, dict) else str(x) for x in s)
    return str(s)


def _fetch_state(host, sid, tokens):
    """Fetch a saved state by id. Returns (server_url, json|None, http_status|err).

    Tries the flywire and global.daf-apis state servers with every token, mirroring
    restore_old_ng_links.fetch. Never raises -- network errors come back as a string
    in the third slot so the batch never crashes.
    """
    servers = STATE_SERVERS
    if host and "flywire-daf.com" in host:
        servers = list(reversed(STATE_SERVERS))
    last = None
    for srv in servers:
        for tok in tokens:
            try:
                r = requests.get(
                    srv + sid, headers={"Authorization": "Bearer " + tok}, timeout=30
                )
                last = r.status_code
                if r.status_code == 200:
                    return srv, r.json(), 200
            except Exception as e:  # noqa: BLE001
                last = str(e)[:60]
    return None, None, last


def _inspect_contents(state):
    """Classify a fetched state by its layer contents.

    Returns (status, detail). Mirrors the routing logic in
    restore_old_ng_links.route / the web tool's swapDeadMicronsEm, but for
    *validation* rather than rewriting.
    """
    layers = state.get("layers", []) if isinstance(state, dict) else []
    segs = [_srcof(L) for L in layers if "segmentation" in (L.get("type") or "")]
    imgs = [_srcof(L) for L in layers if L.get("type") == "image"]

    # A FlyWire prodv1 / fly_v## scene is live but only ngl.flywire.ai can auth it.
    if any("flywire-daf.com" in s or re.search(r"/fly_v\d", s) for s in segs):
        return "ok", "FlyWire scene (live; open in ngl.flywire.ai)"

    # Old microns-seunglab minnie EM images had their voxel data garbage-collected
    # (the info header survives so the layer loads forever as black). Repointable.
    if any("microns-seunglab" in s and "minnie" in s for s in imgs):
        return "dead-em", "dead microns-seunglab minnie EM image (repoint to public EM)"

    # microns-seunglab-hosted segmentation sources are the deprecated seunglab copies.
    if any("microns-seunglab" in s for s in segs):
        return "seg-gone", "deprecated microns-seunglab segmentation source"

    return "ok", "state loads; no dead EM/seg patterns detected"


def _inline_json(input_str):
    """Return the parsed JSON of an inline #!{...} or raw {..} state, else None."""
    text = None
    if input_str.startswith("{"):
        text = input_str
    else:
        idx = input_str.find("#!")
        if idx != -1:
            frag = input_str[idx + 2:]
            try:
                text = urllib.parse.unquote(frag)
            except Exception:
                text = frag
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def classify_link(link, tokens=None, do_fetch=True):
    """Classify a single link's liveness.

    Args:
      link:     the raw link / source / state string.
      tokens:   list of CAVE bearer tokens (from load_tokens()); [] disables fetch.
      do_fetch: if False, classify by shape only (no network).

    Returns a dict:
      {link, shape, status, state_id, host, http_status, detail}
    status is always one of STATUSES. Never raises.
    """
    tokens = tokens or []
    out = {
        "link": link,
        "shape": "unknown",
        "status": "unrecognized",
        "state_id": None,
        "host": None,
        "http_status": None,
        "detail": "",
    }
    try:
        s = (link or "").strip()
        if not s:
            out["detail"] = "empty input"
            return out

        # 1) browser-local scene -- never portable, nothing on any server.
        m = re.search(r"[?&]local_id=([0-9a-f]+)", s, re.I)
        if m:
            out.update(shape="local", status="local-id-unportable",
                       detail="?local_id= scene lives only in the browser that made it")
            return out

        # Pull out the state-server target and id, if any.
        json_url_m = re.search(r"[?&]json_url=([^&#\s]+)", s)
        state_target = urllib.parse.unquote(json_url_m.group(1)) if json_url_m else s
        host_m = re.match(r"https?://([^/]+)", state_target)
        host = host_m.group(1) if host_m else None
        out["host"] = host
        retired = bool(RETIRED_HOST_RE.search(s)) or bool(RETIRED_HOST_RE.search(state_target))

        id_m = re.search(r"nglstate/(?:api/v1/)?(\d+)", state_target)
        bare_id = s if re.fullmatch(r"\d{6,}", s) else None
        state_id = bare_id or (id_m.group(1) if id_m else None)

        # 2) inline / raw JSON state with NO stored-state id -> self-contained.
        if state_id is None:
            inline = _inline_json(s)
            if inline is not None:
                out["shape"] = "inline" if not s.startswith("{") else "json"
                status, detail = _inspect_contents(inline)
                # Self-contained states never touch the retired state server, so a
                # clean one is genuinely ok; only content problems downgrade it.
                out.update(status=status, detail="self-contained; " + detail)
                return out

            # 3) raw graphene:// source string.
            if "graphene://" in s:
                fly = bool(re.search(r"/fly_v\d|flywire-daf\.com", s))
                out.update(shape="raw-source", status="ok",
                           detail=("FlyWire source (needs middleauth in viewer)"
                                   if fly else "graphene source (needs middleauth)"))
                return out

            # 4) short link / bare nglstate URL to a live server but no id parsed.
            if json_url_m or re.match(r"https?://[^\s]*nglstate", s, re.I):
                out.update(shape="short",
                           status="dead-host" if retired else "auth-gated",
                           detail="stored-state link; id not parseable from URL")
                return out

            out["detail"] = "no neuroglancer/state link recognized"
            return out

        # --- We have a stored-state id. ---
        out["state_id"] = state_id
        out["shape"] = "bare-id" if bare_id else "state"

        # 5) truncated id beats everything -- the fetch would just 404 spuriously.
        if len(state_id) < MIN_STATE_ID_DIGITS:
            out.update(status="truncated-id",
                       detail="state id is %d digits; real CAVE ids are ~16 (truncated?)"
                              % len(state_id))
            return out

        # 6) fetch + inspect contents when we can.
        if do_fetch and tokens:
            srv, state, http = _fetch_state(host, state_id, tokens)
            out["http_status"] = http
            if state is not None:
                status, detail = _inspect_contents(state)
                out.update(status=status, detail=detail)
                return out
            if http in (401, 403):
                out.update(status="auth-gated",
                           detail="state server returned %s with our token" % http)
                return out
            if http == 404:
                out.update(status="dead-host" if retired else "seg-gone",
                           detail="state not found (404) on the live server")
                return out
            out.update(status="dead-host" if retired else "auth-gated",
                       detail="fetch failed (%s)" % http)
            return out

        # 7) no fetch -- classify by shape.
        out.update(status="dead-host" if retired else "auth-gated",
                   detail="not fetched (%s)" % ("no token" if not tokens else "--no-fetch"))
        return out
    except Exception as e:  # noqa: BLE001  -- never crash the batch
        out.update(status="unrecognized", detail="classify error: " + str(e)[:80])
        return out


def main():
    ap = argparse.ArgumentParser(
        description="Validate liveness of neuroglancer / appspot / CAVE-state links.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Statuses: " + ", ".join(STATUSES),
    )
    ap.add_argument("--input", "-i", required=True,
                    help="Text file of links (one per line or concatenated).")
    ap.add_argument("--output", "-o",
                    help="Report path (.json). Default: <input>_validated.json")
    ap.add_argument("--no-fetch", action="store_true",
                    help="Shape-only classification; do not hit the network.")
    args = ap.parse_args()

    text = open(args.input, encoding="utf-8").read()
    links = split_links(text)
    tokens = [] if args.no_fetch else load_tokens()
    if not args.no_fetch and not tokens:
        print("No CAVE token found in ~/.cloudvolume/secrets/ -- running shape-only.",
              file=sys.stderr)

    out_path = args.output or (
        os.path.splitext(args.input)[0] + "_validated.json")

    print("%d links to validate\n" % len(links))
    results = []
    counts = {}
    for i, link in enumerate(links, 1):
        r = classify_link(link, tokens=tokens, do_fetch=not args.no_fetch)
        results.append(r)
        counts[r["status"]] = counts.get(r["status"], 0) + 1
        print("%3d  %-19s %-14s %s" % (
            i, r["status"], (r["state_id"] or "-"), r["detail"][:60]))

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"count": len(results), "counts": counts, "results": results},
                  f, indent=2)

    print("\nSummary:")
    for st in STATUSES:
        if counts.get(st):
            print("  %-20s %d" % (st, counts[st]))
    print("\nwrote %s" % out_path)


if __name__ == "__main__":
    main()
