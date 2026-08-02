# DATASET_WIRE report, Bε on the real Dataset commit path

**Goal file** [DATASET_WIRE.md](./DATASET_WIRE.md), executed 2026-08-02 on
`research/jack-betree-extend`. Baselines,
[update_ab_local_20260801.csv](./bench-results/update_ab_local_20260801.csv)
for the research adapter,
[txn_parity_local_20260802.csv](./bench-results/txn_parity_local_20260802.csv)
for the txn-aware adapter, and
[root_delta_local_20260802.csv](./bench-results/root_delta_local_20260802.csv)
for the delta chain.

## Verdict

**PASS.** Fine-grained add-column and replace commits keep the
update-window byte property on the real `lance::Dataset` commit path.
Contiguous F=10 add-column trickle at N=50K totals 4,847 bytes per commit,
tree plus production transaction file plus root delta, against a kill line
of 39,690 bytes and flat's 4,477,891. Replace totals 4,598 bytes. The
production transaction files carry only `Append` fragments or
`DataReplacement` groups, never a fragment list, pinned by a Dataset-level
test. Point resolve stays at 2 GETs. The measured contender is
`CommitBuilder::execute` with real transactions; the research adapter and
the `lance-table` research commit builder are out of the harness entirely,
and the old research-adapter bench target is deleted.

## Where the real path branches

`CommitBuilder::execute_inner` in
`rust/lance/src/dataset/write/commit.rs` dispatches to the Bε backend
immediately after destination resolution and before any flat manifest
machinery. Three rules decide the route, and flat datasets pay nothing new.

1. A `Dataset` destination whose `manifest.config` carries
   `lance.manifest.layout=betree` commits through the Bε backend. This is
   how the handle a Bε commit returns chains into the next commit, config
   check only, no IO.
2. A `Uri` destination is loaded with `DatasetBuilder` exactly as before.
   Only when that load fails with not-found does the branch probe
   `_bt/root/` with one bounded listing. A Bε dataset has no `_versions/`
   manifests, so this is the reopen path; a flat dataset loads normally
   and never reaches the probe.
3. A create, nothing at the uri, routes to the Bε bootstrap only when the
   Overwrite's `config_upsert_values` selects `lance.manifest.layout=betree`
   through `ManifestLayout::from_config`, which also rejects unknown layout
   values with a clear error. Overwriting an existing flat dataset with the
   betree layout errors instead of silently poisoning the flat manifest's
   config.

The backend itself lives in `rust/lance/src/dataset/betree.rs`.
`execute_create` bootstraps the tree from the Overwrite fragments and
stores a manifest skeleton, schema plus config with an empty fragment
list, in the root. `execute_commit` reopens the latest tree, writes the
production transaction file under `_transactions/` via the same
`write_transaction_file` the flat path uses, translates the operation to
fragment actions, and publishes through the tree's create-only root CAS,
with conflict retry that deletes the stale transaction file and replays
the actions against the new tip. Both return a real `Dataset` constructed
through `Dataset::checkout_manifest`, carrying schema, config, and
version, with an empty fragment list. That handle is a commit handle;
reads go through `lance::dataset::betree::Reader` until scanner adoption.

## How operations map to actions without a full-list Merge

No measured operation carries a fragment list.

| Dataset operation | Transaction payload | Bε actions |
|-------------------|--------------------|------------|
| Append | new fragments only | `AddFragment` per fragment with known aggregate deltas, zero reads |
| Add-column backfill | `DataReplacement` groups, `(fragment_id, DataFile)` with new field ids | `ReplaceDataFile` per group |
| Replace file | `DataReplacement` groups, `(fragment_id, DataFile)` matching existing fields and file version | `ReplaceDataFile` per group |
| Delete | deleted ids plus updated fragments | `RemoveFragment` and fragment upserts |
| Create | Overwrite, whole table by definition | tree bootstrap |

The key move is a new buffered engine action, `ReplaceDataFile`, that
carries production `Operation::DataReplacement` semantics to the leaf: an
existing file matching the replacement's fields and file version swaps
path, size, and base id in place; a replacement whose fields are disjoint
from every existing file is appended, which is exactly the add-column
special case production already defines; partial overlap is an error. The
decision runs at leaf application, so a fine-grained commit performs no
reads. One semantics difference is stated plainly: production validates a
DataReplacement against materialized fragments at commit time, while the
Bε path defers that validation to when the action reaches a leaf, so a
malformed replacement fails a later flush rather than its own commit.
`Operation::Merge` is untouched and unwired; schema-evolution APIs that
route through it today would need the fine-grained shape before adopting
this path, and nothing in the measured scenarios uses it.

## Byte tables on the Dataset path

N=50K, F=10, 128 KiB budget, root delta tail 32, median of three runs,
caches off, local filesystem. Bε total is tree plus production transaction
file plus root delta, split per commit from the store's built-in IO
tracker and verified against known component sizes on a smoke
configuration. Baselines are the research-adapter runs from the same
budget and delta policy.

Contiguous locality.

| Scenario | Dataset-path total B | Research root-δ total B | Flat total B | Dataset ms | Flat ms |
|----------|---------------------:|------------------------:|-------------:|-----------:|--------:|
| Add-column trickle, 500 commits | 4,847 | 3,969 | 4,477,891 | 2.93 | 6.67 |
| Replace trickle, 500 commits | 4,598 | 4,199 | 4,283,604 | 2.91 | 6.50 |
| Mixed stream, 1,000 commits | 3,846 | 3,405 | 4,409,964 | 4.66 | 6.61 |

Scattered locality.

| Scenario | Dataset-path total B | Research root-δ total B | Flat total B |
|----------|---------------------:|------------------------:|-------------:|
| Add-column trickle | 6,376 | 5,489 | 4,477,891 |
| Replace trickle | 6,415 | 8,876 | 4,283,604 |

Three observations worth stating. First, the Dataset path costs roughly
900 bytes per commit more than the research chain because the production
transaction file and the delta root each carry the commit's actions;
consolidating them into one object is a known follow-up, not a blocker.
Second, scattered replace got cheaper than the research baseline, 6,415
versus 8,876 bytes, because one `ReplaceDataFile` action replaces the old
remove-plus-add pair, halving buffered bytes and folds. Third, commit
latency is about 2.9 ms versus the research adapter's 0.43 ms because
every `CommitBuilder` commit is stateless and reopens the tree, compacted
root plus outstanding delta tail, before publishing. A session-cached
tree, the same caching production applies to flat manifests, would remove
most of that; it still beats flat's 6.5 to 6.7 ms while writing about
900× less.

Reads after the streams stayed at 2 resolve GETs everywhere. Open with an
outstanding tail cost 26 GETs after trickle, 33 after replace, and 13
after mixed. The append read matrix opened in 3 GETs / 0.8 ms and
materialized 50,100 fragments in 68 GETs / 24 ms.

## Kill criteria evaluation

1. **Contiguous trickle and replace totals within 10× of the root-delta
   baseline, kill above roughly 39,690 bytes.** Trickle 4,847, replace
   4,598, both about 1.2× the research baseline and two orders of
   magnitude inside the line. Pass.
2. **Production transaction must not encode a fragment list.** The
   measured commits write `Operation::Append` with new fragments only or
   `Operation::DataReplacement` with `(fragment_id, DataFile)` groups.
   The `production_transaction_file_stays_action_scale` test decodes the
   file and pins a 10-group transaction under 4 KiB at N=2,000. No
   `Merge` appears anywhere in the path. Pass.
3. **Uncached point resolve at most 4 GETs.** 2.0 GETs in every scenario.
   Pass.
4. **The measured path must not be the research adapter.** The bench
   commits exclusively through `CommitBuilder::execute`; the lance-table
   `betree_ab` target is deleted and the bench header documents the ban.
   Pass.

## What is still not wired

- `Dataset::open`, scanner, `count_rows`, and every index call site. Flat
  readers fail closed on Bε datasets, no `_versions/` manifests exist to
  read, but the `Dataset` handle a Bε commit returns reports an empty
  fragment list to any read API that ignores its config. Scanner adoption
  is the next integration step and the biggest one.
- `Operation::Merge`, `Update`, `Overwrite`-of-existing, `Rewrite`, and
  index operations error with a clear not-wired message.
- Semantic conflict checks on retry. The Bε path replays actions against
  the new tip, last writer wins at the leaf; production's
  `TransactionRebase` checks are not consulted.
- Schema evolution metadata. `DataReplacement` attaches the backfill
  files, but the manifest skeleton's schema is not extended with the new
  fields yet; production add-column also updates the schema.
- Python and Java bindings, compaction, S3 evidence on this branch.

## Paste-ready blurb

> Follow-up on the open question of whether the Bε numbers survive the
> real writer. We wired lance.manifest.layout=betree into
> CommitBuilder::execute itself, create via Overwrite config, append via
> Operation::Append, add-column and file replacement via
> Operation::DataReplacement, real _transactions/ files, flat default
> untouched and unknown layouts rejected. On that path, contiguous F=10
> add-column trickle at N=50K writes 4.7 KiB per commit all-in, tree plus
> transaction plus root delta, versus flat's 4.4 MiB, and replace writes
> 4.5 KiB versus 4.2 MiB. Point resolve stays at 2 GETs. The transaction
> files carry only the touched groups, no fragment list, which we pin
> with a test. Commit latency is 2.9 ms against flat's 6.6 ms locally,
> with a known 900-byte-per-commit consolidation opportunity between the
> transaction file and the delta object. Not landed and not claimed to
> be: scanner, indexes, Merge-shaped schema evolution, semantic conflict
> rebase, and S3 evidence are still open, and the returned Dataset handle
> is a commit handle only. But the update-window property is no longer a
> research-adapter result.

## Artifacts

- [bench-results/dataset_wire_local_20260802.csv](./bench-results/dataset_wire_local_20260802.csv), 42 rows, both localities plus the read matrix
- `rust/lance/src/dataset/betree.rs`, the commit backend, layout detection, knob keys, and `Reader`, with five Dataset-level tests
- The `execute_inner` dispatch in `rust/lance/src/dataset/write/commit.rs`
- Engine additions in `lance-table`, the `ReplaceDataFile` action with production DataReplacement semantics and `BeTree::commit_append`
- `rust/lance/benches/betree_ab.rs`, the Dataset-backed harness; the lance-table research-adapter bench target is deleted, which also retires the compression probe whose numbers stand recorded in [OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md)
- Reproduce with the same env vars as before against `cargo +1.97.0 bench -p lance --bench betree_ab`
