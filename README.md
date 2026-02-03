# Tracer Tools - Extensions

Performance-oriented extensions to Princeton tracer tools, focused on **batched and parallelized workflows** for large-scale proofreading tasks.

This repository is **not a full fork** of the original tracer tools. It exists to collect **known-good extensions and faster variants** of specific scripts that become bottlenecks at scale-particularly when working with thousands of IDs.

---

## Why this repo exists

Some tracer workflows work well for small inputs but become slow or impractical when:

- Updating or resolving **thousands of IDs**
- Fetching or updating **large batches of coordinates**
- Making many sequential API calls that can be safely optimized

The scripts in this repository address those issues by introducing:

- **Batching** (fewer, larger API requests)
- **Threaded / parallel execution** where safe
- Clear separation between batching, I/O, and post-processing

In practice, these extensions reduce runtimes from **tens of minutes to a few minutes** for ~5,000 IDs.

---

## Scope (intentionally limited)

Only scripts that meet **all** of the following criteria live here:

- Derived from or inspired by existing Princeton tracer tools
- Functionally equivalent (same outputs, faster execution path)
- Proven useful in real proofreading workflows
- Focused on performance improvements, not feature expansion

Experimental, incomplete, or one-off scripts do **not** belong in this repository.

---

## Included tools

### `fast_validate_ids.py` — Batched / Parallel ID Validator
- Resolves updated or canonical IDs from large input lists
- Uses batching and threading to avoid slow sequential calls
- Designed for thousands of IDs per run

### `fast_get_coords.py` — Batched Coordinate Getter
- Efficiently fetches coordinates at scale
- Avoids per-ID request overhead
- Safe for large proofreading datasets

These are the **stable, high-value extensions**-the ones that are fast, reliable, and worth keeping.

---

## What's not included

- Full tracer tool suites
- UI or visualization tools
- Debugging or exploratory scripts
- Deprecated or superseded versions
- Anything that has not demonstrated real-world utility

For baseline or canonical implementations, refer to the upstream Princeton tracer tools.

---

## Relationship to upstream tools

- This repository **does not replace** upstream tracer tools
- It does **not** aim to stay in lockstep with upstream changes
- Internal implementations may diverge as long as outputs remain correct

Think of this repository as an **extension and performance layer**, not a canonical source of truth.

---

## Intended audience

- Proofreaders and analysts working with **large ID sets**
- Power users who routinely hit performance limits
- Anyone who wants **drop-in faster extensions** of established workflows

If your workloads are small, you may not need these tools.

---

## Usage notes

- Scripts are designed to be run **as-is**
- Configuration is kept minimal and explicit
- Batching and threading parameters are surfaced where appropriate
- Read the header comments in each script before running

Always test on a small subset before running on critical data.

---

## Stability & guarantees

- Scripts in this repository are used in production workflows
- Performance improvements are intentional and measured
- Correctness is prioritized over maximum concurrency

That said, no script is risk-free-use appropriate caution.

---

## License & attribution

These tools are derived from Princeton tracer tooling and internal workflows.  
Attribution is preserved where applicable.

---

## Development note

Some implementation details were developed with AI assistance and subsequently reviewed and validated by a human.


