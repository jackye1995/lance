# Draft reply for discussion #7499

Copy the fenced block below into GitHub. The outer fence is only for transport.
Remove it before posting, or paste the inner markdown as-is from a preview.

````markdown
@jackye1995 your post changed my working hypothesis, so I extended the prototype and tried to falsify the Bε advantage with a matched flat / tiered / Bε harness.

Controls: fabricated metadata only, local filesystem, Rust 1.97 + Cargo `bench`, fanout 16, shared 128 KiB budget for Bε nodes and the tiered message cap, caches off for both lazy layouts, median of three independent runs. Timing starts from the same prebuilt request. Tiered bytes include its real transaction file; Bε still has no production txn record.

Most informative case: N=50K, F=10, 500 contiguous commits at 128 KiB.

| Layout |     KiB/commit | ms/commit |
| ------ | -------------: | --------: |
| flat   |        4,372.9 |      6.93 |
| tiered | 9,227.3 total† |     69.07 |
| Bε     |           61.0 |      0.50 |

† Approximately 4,854.3 KiB of tree output plus 4,372.9 KiB of transaction output. Displayed components are independently rounded.

That result reproduced after a targeted review in which I fixed a selective-read routing bug and corrected internal-node size accounting. The routing bug affected bitmap reads, not the headline write result. I included the rerun so the reported numbers reflect the corrected implementation.

My reading is narrower than “all tiered trees lose.” In this experiment, Bε behaved like an update-window-scale structure, while flat rewrote table-scale metadata. The current tiered transaction separately serializes the complete fragment list, and its tree path also produced approximately 4.85 MiB per commit.

Those are findings about these implementations, not proof that every tiered design must lose.

### Where Bε loses

In the whole-table one-shot case, touching all 50K entries, flat finishes in about 14 ms at 7,943 KiB. Bε writes less, around 4,540 KiB, but takes about 168 ms.

Scattered trickle still stays below flat, at 92.4 KiB and 0.69 ms versus 4,373 KiB and 8.67 ms, but the gap narrows compared with contiguous updates.

### Read tradeoff at roughly 100K fragments

Caches disabled:

| Read at ~100K |            tiered |    Bε 128/128 |   Bε 64/1024 |
| ------------- | ----------------: | ------------: | -----------: |
| materialize   | 20 GETs / 41.8 ms | 136 / 61.8 ms | 16 / 32.5 ms |
| point resolve |       2 / 3.12 ms |   2 / 0.47 ms |  1 / 1.57 ms |

Matched 128/128 leaves more objects than tiered. Larger leaves reduce materialization work, but increase uncached point latency.

I have the harness, full matrix, three-run CSVs, and regressions ready to share.

The next experiment I’d prioritize is either object-store runs with realistic latency, or per-node immutable message runs, rather than only a root delta chain, to see whether internal-buffer rewrites can shrink without giving back read amplification.

Which would be more useful for #7848?
````
