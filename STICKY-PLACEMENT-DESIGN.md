# Sticky placement — design (relocate only the culprit on redeploy)

**Status: IMPLEMENTED 2026-06-08.** Authored 2026-05-21.
**Goal:** when the runtime monitor flags a slow node, the redeploy should relocate **only the operators on that node**, leaving every other operator on its current host. (Pre-2026-06-08 it reshuffled the whole job.)

## Implementation notes (what was built vs what the design proposed)

The v1 implementation matches §1–§5 of the design. Two refinements at the integration layer where the design left choices open (§6, §6b):

1. **Single-writer-monitor instead of two-writer + file lock.** §6 described both `GraphMlWriter.setExcluded` (Java) and `generate-graphml.py` (Python) writing the graphml, with a "merge: preserve `excluded`" hand-wave for the periodic re-bench. To avoid the inherent read-modify-write race between two writers, the monitor became the *sole* writer of `cloud.graphml` post-bootstrap: the periodic re-bench is now a Java task (`BenchRunner`) that SSHes to workers via `bench-cluster.sh`, parses `bench-results.txt`, then calls `GraphMlWriter.updateCapabilities(Map)` — which only mutates `compute` attributes and leaves `excluded` flags intact by construction. `generate-graphml.py` runs only at cold-start (bootstrap) and now emits the `<key id="excluded">` declaration. Operational consequence: do **not** run `bench-cluster.sh + generate-graphml.py` manually while the monitor is up — it would race with the monitor.
2. **`ExclusionState`** is the in-memory bookkeeping that backs cooldown-based auto-recovery (§6b). It's pre-loaded from the graphml on monitor startup (so a monitor restart doesn't lose track of pre-existing `excluded` hosts; cooldown timer restarts from "now" in that case).
3. **v1 skips probation** for freshly-recovered nodes (per design §6b's allowance) — the detector re-excludes if the node is genuinely still bad.
4. **`shrink-factor` / `min-capability`** stay parseable for back-compat but the controller ignores them. New config keys added: `e2c.redeploy.recovery-cooldown` (5 min default), `e2c.bench.interval` (10 min default), `e2c.bench.script`, `e2c.bench.results-path`, `e2c.bench.timeout`. Flink side: `cluster.placement.sticky-state-path` (defaults to a sibling of `topology.graphml.path` named `placement-state.tsv`).

The first cluster-side validation (Q1 with throttle test) was performed 2026-06-08 immediately after implementation. See the monitor `RUNBOOK.md` "Sticky placement validation" section for the experimental procedure.

---

## 1. Why it reshuffles today

`TopDownBottomUpExecutionGraphPlacement.assignPlacement()`
([flink-runtime/.../scheduler/adapter/TopDownBottomUpExecutionGraphPlacement.java](flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/adapter/TopDownBottomUpExecutionGraphPlacement.java)) is **stateless and positional**:

1. Build operator order along the dataflow: `finalOpOrder = [Source, Calc2, Watermark, MiniBatch, Calc5, Sink]`.
2. Sort compute nodes by `computeCapability` desc.
3. Assign operators positionally, filling each node's `numSlots` (=2) before advancing:

```
op[0,1] → sortedNode[0]   Source, Calc2        → 182
op[2,3] → sortedNode[1]   Watermark, MiniBatch → 185   (culprit)
op[4,5] → sortedNode[2]   Calc5, Sink          → 183
```

It has **no memory of the previous placement**. So when the monitor demotes 185, the
capability sort reorders, every position shifts, and operators that were fine get
remapped. Even *excluding* 185 doesn't help: dropping it from the list slides
positions 2..5 up, so 183 inherits 185's operators and Calc5/Sink move again. No
monitor-side capability trick avoids this — the positional fill guarantees a reshuffle.

**Conclusion:** "only replace the culprit" requires the placement to be **sticky** —
to honour the prior operator→host mapping and re-place only the excluded node's
operators (plus genuinely new operators).

---

## 2. Design overview

Two coordinated changes:

| Component | Change |
|---|---|
| **Placement** (`TopDownBottomUpExecutionGraphPlacement`) | Persist its last assignment to a sidecar file. On the next run: keep each operator on its previous host if that host is still eligible and has a free slot; only unpinned operators (excluded node's + new) go through capability-sort onto remaining slots. Skip nodes marked `excluded`. |
| **Monitor** (`GraphMlWriter` + `PlacementController`) | On flag, **mark the culprit `excluded`** in the graphml instead of halving its `computeCapability`. Capability stays at the true hardware value. |

This also fixes a side problem: today the redeploy permanently corrupts `computeCapability` (we had to re-bench to reset). With an explicit `excluded` flag, capability is left intact — only a policy bit toggles.

---

## 3. Identifying "the same operator" across resubmits

A resubmit is a fresh `ExecutionGraph` with new `JobVertexID`s, so we key the sticky
map on the **operator name** (`JobVertex.getName()`), which is stable when the same
query is replayed (Nexmark names carry unique `[N]` suffixes, e.g. `Calc[2]`,
`WatermarkAssigner[3]`).

Guards:
- If a name occurs more than once in `finalOpOrder`, the match is ambiguous → treat
  those operators as **unpinned** (fall back to positional) and log a warning.
- If the previous state file's operator set doesn't overlap the current job (different
  query), nothing pins → pure positional placement (today's behaviour). Safe.

---

## 4. State file

A sidecar next to the graphml, written by the JM after each placement. TSV (no JSON
lib on the runtime classpath):

```
# placement-state.tsv  —  <hostId>\t<operatorName>
10.10.2.182	Source: datagen[1]
10.10.2.182	Calc[2]
10.10.2.185	WatermarkAssigner[3]
10.10.2.185	MiniBatchAssigner[4]
10.10.2.183	Calc[5]
10.10.2.183	nexmark_q1[6]: Writer
```

- Path: `graphMlPath` sibling, e.g. `/mnt/labstore/aelmansoury/conf/placement-state.tsv`.
- Single writer (the JM during placement); no concurrency concern.
- Stale entries (host no longer in topology / name not in current job) are ignored.

---

## 5. Placement algorithm (new `assignPlacement`)

```
1. finalOpOrder        = dataflow order            (unchanged)
2. eligibleNodes       = compute nodes where excluded != true, capability-sorted
3. slots[node]         = node.numSlots
4. prev                = load placement-state.tsv   (name -> host), or empty
5. pinned              = {}
   // Pass 1 — sticky
   for op in finalOpOrder:
       host = prev[op.name]
       if host != null && host in eligibleNodes && slots[host] > 0 && name-unique(op):
           assign op -> host;  slots[host]--;  pinned.add(op)
   // Pass 2 — fill remaining (the excluded node's ops + new ops)
   idx = 0
   for op in finalOpOrder where op not in pinned:
       advance idx past eligibleNodes with 0 slots
       if idx >= eligibleNodes.size: throw "Not enough free slots for placement"
       assign op -> eligibleNodes[idx];  slots[eligibleNodes[idx]]--
6. persist new (name -> host) to placement-state.tsv
```

Properties:
- Operators on **non-excluded** nodes keep their host (Pass 1). ✓
- Only the **excluded** node's operators (their previous host is ineligible) and
  brand-new operators are placed in Pass 2. ✓
- **First placement** (no state file): Pass 1 pins nothing → Pass 2 = today's behaviour. ✓

### Worked example (throttle 185)
Prev: `182→{Source,Calc2}`, `185→{Watermark,MiniBatch}`, `183→{Calc5,Sink}`.
Monitor marks `185 excluded`. Resubmit:
- Pass 1 pins Source,Calc2→182 and Calc5,Sink→183 (hosts eligible, slots free).
  Watermark,MiniBatch can't pin (185 excluded).
- Pass 2 places Watermark,MiniBatch onto the best eligible node with free slots,
  e.g. 186.
- Result: **182 and 183 unchanged; only 185's two operators moved to 186.** ✓

---

## 6. Monitor-side changes

- **`GraphMlWriter`**: add `setExcluded(Collection<String> hosts, boolean excluded)` —
  rewrites the graphml adding/removing an `excluded` attribute on the matching node(s),
  leaving `computeCapability` untouched. (Atomic write + symlink-resolve as today.)
- **`PlacementController.handleSlowNodes`**: replace `graphMlWriter.shrinkCapabilities(...)`
  with `graphMlWriter.setExcluded(flagged, true)`. Cancel + resubmit flow unchanged.
- **Config**: `e2c.redeploy.shrink-factor` / `min-capability` become unused under the
  exclude model. Either keep for back-compat or add `e2c.redeploy.mode: exclude|shrink`
  (default `exclude`).
- **Recovery** (when does an excluded node rejoin?): see §6b — auto-recover after a
  cooldown, with periodic re-bench keeping capability fresh. Dashboard should render
  excluded nodes distinctly.

### GraphML `excluded` attribute
The generator (`scripts/generate-graphml.py`) and the placement reader both need to
know the key. Add a GraphML `<key id="excluded" for="node" attr.type="boolean">` and
read it in `createTopologyNode` (default `false`). `ComputeNode` gains an `excluded`
field; step 2 filters on it.

---

## 6b. Capability freshness — periodic re-bench + recovery

The bench is a one-time snapshot (`openssl sha256`, 3 s/node, normalized fastest=1.0). Two
problems follow from that, both surfaced 2026-05-25:

1. **Stale display (already fixed).** The monitor cached `compute` at startup and never
   re-read it, so a redeploy demotion (graphml `compute` 0.99→0.49) never showed on the
   dashboard. **Fixed**: `E2cMonitor.maybeReloadCapabilities()` re-reads `cloud.graphml`
   whenever its mtime changes (single-threaded in `sampleOnce`). Under the exclude-flag
   model `compute` stops moving anyway, but the reload still matters so a **periodic
   re-bench** is reflected live.
2. **Stale placement input.** Placement reads `cloud.graphml` fresh per submission, but the
   file holds bench-time `compute` ± demotions — never re-measured. If a node degrades after
   bench, its `compute` stays high and a redeploy can place work on a now-slow node. The
   runtime detector catches it (throughput-trend) and demotes → another redeploy, so it
   self-corrects, but it can cost an extra cycle.

**Three concepts, kept separate** (the model this design moves to):

| Concept | Meaning | Source | Lever |
|---|---|---|---|
| `compute` (capability) | how fast the node *can* go | bench (periodic) | re-bench only |
| `excluded` (health/policy) | "avoid this node" | detector | redeploy sets/clears |
| runtime load/throughput | what it's doing now | senders + REST | read-only signal |

**Periodic re-bench.** A scheduler (cron, or a monitor-internal timer) re-runs
`bench-cluster.sh` + `generate-graphml.py` every N minutes (e.g. 10–15) to refresh
`compute` and catch hardware drift. Notes:
- It briefly competes with the running job for CPU (3 s/node). Run it staggered / at low
  load, or accept the small blip — it's measuring max CPU, which is the point.
- It **must preserve `excluded` flags** (don't un-exclude a node just because we re-benched).
  So `generate-graphml.py` should merge: write fresh `compute`, but carry forward the
  current `excluded` set (read the existing graphml first, or read an exclusions sidecar the
  monitor maintains).
- The monitor picks up the new `compute` automatically via the mtime reload (§6b.1).

**Recovery (un-exclude after cooldown).** An excluded node shouldn't be benched forever for a
transient blip:
- After `e2c.redeploy.recovery-cooldown` (e.g. 5 min) since a node was excluded, the monitor
  clears its `excluded` flag (`setExcluded(host, false)`) so it rejoins the candidate pool on
  the next placement.
- Optional safety: before fully trusting it, the next redeploy that *would* use it can treat
  a freshly-recovered node as lowest-priority for one cycle (probation), re-flagging
  immediately if it's still slow. v1 can skip probation and rely on the detector to re-exclude
  if the node is genuinely bad.
- New config: `e2c.redeploy.recovery-cooldown` (Duration, e.g. `5 min`; `0` = never
  auto-recover, i.e. manual-only).

This pairs cleanly with the exclude model: `compute` is owned by the (periodic) bench;
`excluded` is owned by detect→cooldown→recover. Neither corrupts the other.

---

## 7. Build & deploy (the heavy part)

- Files touched in the fork: `TopDownBottomUpExecutionGraphPlacement.java`
  (sticky logic + read `excluded`), possibly `ClusterOptions` (if a config key is added).
- Rebuild the `flink-runtime` module and repackage the dist so
  `build-target/lib/flink-dist*.jar` (or the runtime jar) carries the change:
  - confirm the exact build the cluster's `build-target` was produced from
    (full `mvn package` vs module build + dist assembly).
  - Flink builds are long (~10–40 min) vs the monitor jar's seconds.
- Restart the cluster (JM + TMs) to load the new runtime — and **restart the
  `CpuMetricSender`s afterward** (TMs get new PIDs; see RUNBOOK "CPU reads 0 after a
  partial restart").
- Monitor jar change (GraphMlWriter/PlacementController) deploys as usual (seconds).

---

## 8. Validation plan

1. Submit Q1, record placement (e.g. 182/185/186).
2. Throttle 185 → flag → monitor marks `185 excluded` → cancel → resubmit.
3. **Expect:** 182 and 186 keep their operators; **only** Watermark+MiniBatch (185's)
   relocate to a non-excluded node (e.g. 183). Verify via `status.json` `placement`.
4. Re-bench graphml → confirm `185` is eligible again (exclusion cleared) and a fresh
   submission places normally.
5. Restart-safety: kill the JM, resubmit — first placement has no/old state file →
   positional fallback works.

---

## 9. Open questions / risks

- **`numSlots` in the generated graphml.** Sticky relies on per-node slot counts.
  Observed placement puts 2 operators per node, so the graphml must set `slots=2`
  (the reader defaults to 1). Confirm `generate-graphml.py` emits `slots`.
- **Operator-name stability.** Confirm Nexmark/Flink keep identical `JobVertex` names
  across resubmits of the same query (expected — names are derived from the SQL plan).
- **Capacity after exclusion.** Excluding 1 of 6 nodes leaves 5×2=10 slots for 6
  operators — fine for Q1. A job needing >10 slots would fail (same failure mode as today).
- **Multiple culprits.** If >1 node is flagged, all are excluded; their operators
  compete for remaining slots in Pass 2. Works as long as capacity remains.
- **Recovery policy** beyond manual re-bench is out of scope for v1.
