# Overnight: TXN_PARITY then ROOT_DELTA

**Worktree:** `/Users/viltrum/workspace/lance-betree-extend`  
**Branch:** `research/jack-betree-extend`  
**Do not commit, push, or post to GitHub unless the human explicitly asks.**

Hand this file to a fast agent as the overnight umbrella.

---

## Order

```text
1. TXN_PARITY.md    ← done (PASS)
2. ROOT_DELTA.md    ← done (PASS, adopt behind flag)
3. DATASET_WIRE.md  ← next: real Dataset / CommitBuilder, no research writer
```

Historical note: if TXN_PARITY had hard-KILLed, ROOT_DELTA would have stopped.
Both passed. The remaining honesty gap is production writer wiring. See
[DATASET_WIRE.md](./DATASET_WIRE.md).

---

## Goals

| # | File | Question |
|---|------|----------|
| 1 | [TXN_PARITY.md](./TXN_PARITY.md) | Does update-window scale survive a real-ish txn record? **PASS** |
| 2 | [ROOT_DELTA.md](./ROOT_DELTA.md) | Does a root delta chain cut per-commit root rewrite without blowing read amp? **PASS** |
| 3 | [DATASET_WIRE.md](./DATASET_WIRE.md) | Does the win survive the **real** Dataset / CommitBuilder writer? |

Prior evidence: [OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md),
[TXN_PARITY_REPORT.md](./TXN_PARITY_REPORT.md),
[ROOT_DELTA_REPORT.md](./ROOT_DELTA_REPORT.md).

---

## Hard rules

- Flat remains default. No production layout default change.
- No sealed-AMT revival.
- No GitHub posts.
- Prefer extending `betree_ab` / `BeTreeDataset` / `BeTree::commit`.
- Every report must include kill-criteria verdict: PASS / KILL / MIXED.
- Keep `cargo test -p lance-table betree` green; fmt + clippy clean on
  touched crates.
- Check CSVs into `devtools/betree/bench-results/` with a date suffix.

---

## Done when

- [x] `TXN_PARITY_REPORT.md` exists with tables + verdict
- [x] `ROOT_DELTA_REPORT.md` exists with before/after + verdict
- [ ] `DATASET_WIRE_REPORT.md` exists; Bε contender is real Dataset path
- [ ] Tests / fmt / clippy clean as required by the child goals

---

## One-shot agent prompt

```text
Next goal only:
  /Users/viltrum/workspace/lance-betree-extend/devtools/betree/DATASET_WIRE.md

Wire Bε through real lance::Dataset / CommitBuilder behind
lance.manifest.layout=betree. Ban research BeTreeDataset / research
CommitBuilder as the measured path.

Worktree: /Users/viltrum/workspace/lance-betree-extend
Branch: research/jack-betree-extend

Do not commit, push, or post to GitHub.
Produce DATASET_WIRE_REPORT.md.
```
