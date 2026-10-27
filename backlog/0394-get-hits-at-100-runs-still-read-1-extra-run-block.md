---
id: "0394"
title: GET hits at ~100 runs still read ~1 extra run block (bloom false positives); key filter only helps misses
status: done
priority: 3
created: "2026-10-06"
updated: "2026-10-12"
depends_on:
    - "0382"
spec_refs: []
---

## Description
Residual from 0382. The 0382 microbench (100 runs/engine, release, MemBlockDevice) shows staged GET **misses** dropped from ~1.10 to 0.019 candidate block reads per GET once the key filter is consulted. **Hits** are unchanged: about 2.10 block reads and about 99.6 bloom probes per GET, p50 about 4.8 µs. The key filter is a superset filter over all covered runs, so it gives no help when the key is present. Every run's bloom is still probed, and false positives at ~1% FPR × 100 runs cost about one extra block read.

Options to evaluate (measure first, short A/B):
- resolve probes newest-first and stop early, if an ordering invariant holds (newer run ⇒ version ≥ for the same key; check rehome/AE replicated writes);
- more bloom bits per key for frozen/large runs;
- per-run attribution in the key filter.

**Acceptance:** 0393 hardware numbers confirm hit-path extra reads matter at ~100 runs. Then pick one option with a measured A/B, or close as not worth it.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier **L0**; see docs/validation.md. This replaces "0393 hardware numbers confirm" with "the R3 samplecheck Get p50/p99 and blocks-read counters show extra reads on the hit path of ≥0.5 block per GET". The A/B of newest-first/early-stop and bloom bits uses the 0382 MemBlockDevice microbench.

## Acceptance criteria

## Work log
- 2026-10-12: closed as not worth it, under the amended acceptance bar. In the R4 full replay (0386 G4), EOF Gets probed 1.31 runs per replica Get. That is an upper bound, since the aediag ran alongside, and it means ≤0.31 extra block per GET, below the ≥0.5 threshold.
