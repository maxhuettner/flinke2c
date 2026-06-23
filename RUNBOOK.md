# FlinkE2C Thesis Cluster — Runbook

How to operate the **FlinkE2C cluster** on the TUDa lab cluster (zs01–zs08).
Audience: future you, post-meeting, post-coffee.

For the **runtime monitor** (CpuMetricSender + E2cMonitor + detection/redeploy pipeline),
see [`flink-runtime-monitor/RUNBOOK.md`](../flink-runtime-monitor/RUNBOOK.md) — separate
repo, separate concerns.

---

## Layout (where things live)

```
Windows dev machine                              zs01 (JobManager)
─────────────────────────────                    ────────────────────────────────────────────
C:\Users\wagdy\git\                              /mnt/labstore/aelmansoury/
 ├─ flinke2c-private        ◄──── git push ──┐    ├─ flinke2c/
 │   (FlinkE2C fork branch                  │     │   ├─ src/                   ← git clone of flinke2c-private
 │    flinke2c-computeCapabiilityPlacement) │     │   │   ├─ flinke2c-conf/     ← single source of truth for configs
 │                                          │     │   │   └─ flink-dist/        ← Maven build output here
 │                                          │     │   ├─ build-target ─────────► src/flink-dist/target/.../flink-2.2-SNAPSHOT
 │                                          │     │   │     (symlink to the built distribution)
 │                                          │     │   │     └─ conf/{config.yaml, cloud.graphml, masters, workers}
 │                                          │     │   │            → symlinks to src/flinke2c-conf/ (set up by deploy-configs.sh)
 │                                          │     │   └─ build.log
 ├─ flink-runtime-monitor   ◄──── git push ─┘    ├─ flink-runtime-monitor/
 │   (own repo, depends on nexmark-flink)         │   └─ flink-runtime-monitor-0.1-SNAPSHOT.jar    (shaded; scp'd from Windows)
 │                                                ├─ conf/
 │                                                │   └─ nexmark.yaml          ← monitor + senders config
 │                                                └─ logs/                      ← monitor + sender output
 │
 └─ flink101                                     zs02..zs08 (TaskManagers)
     (Docker playground, THESIS-PROGRESS.md      ────────────────────────────────────────────
      and demo scripts live here)                 nothing persistent — JM SSHs out to start TMs;
                                                  TMs read everything from /mnt/labstore (shared NFS)
```

Key invariants:

- **`/mnt/labstore/aelmansoury/`** is shared NFS across all zs nodes. Everything thesis-related lives there.
- **`/home`** is per-node (not shared). SSH keys for inter-node auth have to be on each worker individually.
- The branch is the **source of truth for configs**. `git pull` on zs01 is the deploy step.
- The **monitor JAR is built on Windows** and `scp`'d. Rebuilds = re-scp.

---

## One-time setup (already done)

1. SSH keys: Windows → zs0X (`~/.ssh/id_ed25519`, passphrase `1234`) + zs01 → workers (`~/.ssh/id_ed25519`, internal key, public part in each worker's `~/.ssh/authorized_keys`) + zs01 → github.com (`~/.ssh/id_ed25519_github` for cloning private repos).
2. `git clone` of `flinke2c-private` at `/mnt/labstore/aelmansoury/flinke2c/src` on zs01.
3. First Maven build: `mvn package -pl flink-dist -am -DskipTests -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true`, wrapped in `run_exp`.
4. `build-target` symlink created (`flinke2c/build-target` → built dist).
5. `scripts/deploy-configs.sh` run once — symlinks the 4 conf files into the built dist.

Skip ahead unless something broke.

---

## Start the cluster

**Recommended (full bring-up incl. monitor):** use the bootstrap in the monitor repo, which benches each TM, generates `cloud.graphml`, starts Flink, starts monitor + senders.

```bash
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/bootstrap.sh'
```

See [`flink-runtime-monitor/RUNBOOK.md`](../flink-runtime-monitor/RUNBOOK.md) for details.

**Flink-only (skip monitoring stack):**

```bash
ssh zs01 '/mnt/labstore/aelmansoury/flinke2c/build-target/bin/start-cluster.sh'
```

Either path brings up:
- JobManager on zs01 (REST API on port 8081)
- TaskManager on each line of `flinke2c-conf/conf/workers` (zs02, zs03, zs05, zs06, zs07, zs08)

`config.yaml` points `topology.graphml.path` at `/mnt/labstore/aelmansoury/conf/cloud.graphml` (the runtime, bootstrap-generated path), NOT at any path under `flinke2c-conf/`. If you start Flink without first running the bootstrap, the graphml file won't exist and placement will fail. Either run the bootstrap or generate the graphml by hand:

```bash
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/bench-cluster.sh \
       && python3 /mnt/labstore/aelmansoury/scripts/generate-graphml.py'
```

Verify:

```bash
ssh zs01 'curl -s http://localhost:8081/overview'
# Expect: taskmanagers=6, slots-total=12
```

---

## Open the Flink dashboard

In a separate Git Bash terminal (leave it running):

```bash
ssh -N -L 8081:localhost:8081 zs01
```

Then open <http://localhost:8081> in any browser.

---

## Submit a workload

The canonical entry point is `submit-workload.sh` in the monitor repo — see [`flink-runtime-monitor/RUNBOOK.md`](../flink-runtime-monitor/RUNBOOK.md) → "Submit a workload" for the four invocation styles (default / bare jar name / absolute path / SQL file). For Nexmark queries specifically, use `nexmark-submit.sh q1` (`q1`–`q23`).

All explicit invocations are persisted to `/mnt/labstore/aelmansoury/conf/last-workload.txt`, so `PlacementController`'s auto-resubmit replays the actual workload — not a hard-coded default.

Quick sanity check that bypasses the monitor entirely (and so won't be auto-resubmitted on slow-node detection):

```bash
ssh zs01 'FH=/mnt/labstore/aelmansoury/flinke2c/build-target
$FH/bin/flink run -d -p 1 $FH/examples/streaming/TopSpeedWindowing.jar'
```

Note `-p 1` is required: FlinkE2C places per-operator, not per-subtask. See troubleshooting below.

---

## Verify capability-aware placement

```bash
ssh zs01 'grep "Capability-sorted" /mnt/labstore/aelmansoury/flinke2c/build-target/log/*standalonesession*.log | tail -1'
```

Should print the sorted node list, descending by capability, on every job submission.

For the actual operator → host mapping, the easiest path is the dashboard: Jobs → click the job → click a vertex → "Subtasks" tab → Endpoint column.

---

## The runtime monitor (Tasks 4 & 5 — done)

Live and validated end-to-end. The launchers are `start-monitor.sh` on zs01 and `start-sender.sh` on each worker (both in `/mnt/labstore/aelmansoury/`). The recommended bring-up is `bootstrap.sh` (see monitor RUNBOOK) which starts them in the correct order. To start them by hand:

```bash
ssh zs01 'bash /mnt/labstore/aelmansoury/start-monitor.sh'
ssh zs01 'for w in zs02 zs03 zs05 zs06 zs07 zs08; do
    ssh -o BatchMode=yes "$w" "bash /mnt/labstore/aelmansoury/start-sender.sh"
done'
```

Operational details — detection thresholds, redeploy behaviour, status snapshot, dashboard — all live in [`flink-runtime-monitor/RUNBOOK.md`](../flink-runtime-monitor/RUNBOOK.md).

---

## Live dashboard

The monitor exposes a JSON snapshot at `/mnt/labstore/aelmansoury/conf/status.json` (updated every 1 s by `E2cMonitor`). A static HTML page at `/mnt/labstore/aelmansoury/viz/index.html` polls it and renders an SVG topology (SRC → 6 compute nodes → SNK) plus per-host CPU chart + job graph + event log.

```bash
# start the dashboard server on zs01 (or use bootstrap.sh which does it for you)
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/start-viz.sh'

# from your laptop (lab firewall allowing port 8082)
start http://zs01.lab.tuda.systems:8082

# or via tunnel
ssh -N -L 8082:localhost:8082 zs01    # then http://localhost:8082
```

As of 2026-05-21 the dashboard reads per-operator metrics straight from `status.json` (`jobs[].samples[]`), so the previous cross-origin fetch to Flink REST is **gone** — no CORS dependency for the E2C dashboard itself.

We keep `web: access-control-allow-origin: '*'` in `flinke2c-conf/conf/config.yaml` only because it's still useful if you open the **raw Flink web UI** (port 8081) through a tunnel and want any embedded fetches to work. The setting is harmless to keep. A JM restart is required to change it.

We also keep `metrics.fetcher.update-interval: 1 s` here so the JM refreshes REST metric counters every tick (default is 10 s, which was causing the monitor's per-operator throughput to come back as spikes — see `flink-runtime-monitor/RUNBOOK.md` gotchas). Changing this requires a JM restart, and because that gives the TMs new PIDs you must also restart the senders.

---

## Stop the cluster

```bash
ssh zs01 '/mnt/labstore/aelmansoury/flinke2c/build-target/bin/stop-cluster.sh'
```

If the monitor + senders + dashboard are running, stop them via pid files / port (never `pkill -f` from an SSH-quoted command — the bash subshell SSH spawns has the pattern in its argv and gets killed too; see `flink-runtime-monitor/RUNBOOK.md` gotchas):

```bash
# monitor
ssh zs01 'PID=$(cat /mnt/labstore/aelmansoury/logs/monitor.pid 2>/dev/null); [ -n "$PID" ] && kill "$PID"; rm -f /mnt/labstore/aelmansoury/logs/monitor.pid'

# dashboard (by port)
ssh zs01 'VIZ_PID=$(ss -lptn "sport = :8082" 2>/dev/null | grep -oP "pid=\K\d+" | head -1); [ -n "$VIZ_PID" ] && kill "$VIZ_PID"; rm -f /mnt/labstore/aelmansoury/viz.pid'

# senders (per-host pid files)
ssh zs01 'for w in zs02 zs03 zs05 zs06 zs07 zs08; do
    ssh -n -o BatchMode=yes "$w" "PID=\$(cat /mnt/labstore/aelmansoury/logs/sender.\$(hostname).pid 2>/dev/null); [ -n \"\$PID\" ] && kill \"\$PID\" 2>/dev/null; rm -f /mnt/labstore/aelmansoury/logs/sender.\$(hostname).pid"
done'
```

---

## Make a change

### Config-only change (`config.yaml`, `cloud.graphml`, `masters`, `workers`)

On Windows:
```bash
cd c:/Users/wagdy/git/flinke2c-private
# edit flinke2c-conf/conf/config.yaml  (or whichever)
git add flinke2c-conf
git commit -m "what changed and why"
git push
```

On zs01:
```bash
ssh zs01 'cd /mnt/labstore/aelmansoury/flinke2c/src && git pull'
```

That's it. The symlinks in `build-target/conf/` already point at the source-of-truth, so the new content is live the moment `git pull` finishes.

**When the change takes effect:**
- `cloud.graphml` — next job submission (re-read by `TopDownBottomUpExecutionGraphPlacement` constructor per job)
- `config.yaml` — next cluster restart (Flink reads it at JM/TM startup)
- `masters` / `workers` — next cluster restart (`start-cluster.sh` reads them)

### Java code change in FlinkE2C

On Windows: edit, commit, push as usual.

On zs01:
```bash
ssh zs01 'cd /mnt/labstore/aelmansoury/flinke2c/src && git pull
# rebuild only the changed module(s) (~2-4 min when incremental; ~30 min cold)
run_exp -m "flinke2c rebuild" -n 0 -t 0:30 -- \
    mvn install -pl flink-runtime -am -DskipTests \
    -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true'
```

**Important**: `build-target/lib/` does **not** contain a separate `flink-runtime-*.jar` — the runtime classes are bundled inside `flink-dist-*.jar` (a non-shaded uber jar). Two ways to deploy the freshly-built classes:

#### Option A — surgical jar update (fast, what we use day-to-day)

Update just the changed `.class` files inside `flink-dist-*.jar`. Validated 2026-06-08 for the sticky-placement deploy (3 source files → 7 classes including inner classes):

```bash
ssh zs01 'set -euo pipefail
SRC=/mnt/labstore/aelmansoury/flinke2c/src
DIST=/mnt/labstore/aelmansoury/flinke2c/build-target/lib/flink-dist-2.2-SNAPSHOT.jar
cp "$DIST" "$DIST.bak.$(date +%Y%m%d-%H%M%S)"        # backup first

STAGE=$(mktemp -d) && cd "$STAGE"
# extract whatever classes you changed (mirror the package dir under STAGE):
mkdir -p org/apache/flink/configuration
unzip -j "$SRC/flink-core/target/flink-core-2.2-SNAPSHOT.jar" \
    "org/apache/flink/configuration/ClusterOptions.class" \
    -d org/apache/flink/configuration/

mkdir -p org/apache/flink/runtime/scheduler/adapter
unzip -j "$SRC/flink-runtime/target/flink-runtime-2.2-SNAPSHOT.jar" \
    "org/apache/flink/runtime/scheduler/adapter/TopDownBottomUpExecutionGraphPlacement*.class" \
    -d org/apache/flink/runtime/scheduler/adapter/

jar uf "$DIST" $(find . -name "*.class")
cd / && rm -rf "$STAGE"'
# then bounce the cluster
ssh zs01 '/mnt/labstore/aelmansoury/flinke2c/build-target/bin/stop-cluster.sh
/mnt/labstore/aelmansoury/flinke2c/build-target/bin/start-cluster.sh'
```

Verify before bouncing: `unzip -p $DIST <class-path> | strings | grep <expected-marker>`.

#### Option B — full flink-dist rebuild (slower but trivially correct)

```bash
ssh zs01 'cd /mnt/labstore/aelmansoury/flinke2c/src
run_exp -m "flink-dist rebuild" -n 0 -t 0:45 -- \
    mvn package -pl flink-dist -am -DskipTests \
    -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true
# re-link build-target to the new built dist (path includes version)
ln -sfn $PWD/flink-dist/target/flink-2.2-SNAPSHOT-bin/flink-2.2-SNAPSHOT \
       /mnt/labstore/aelmansoury/flinke2c/build-target
bash $PWD/scripts/deploy-configs.sh
/mnt/labstore/aelmansoury/flinke2c/build-target/bin/stop-cluster.sh
/mnt/labstore/aelmansoury/flinke2c/build-target/bin/start-cluster.sh'
```

**After any partial Flink restart, also restart the senders** — TMs come up with new PIDs and the senders silently read 0 from the old PIDs (see monitor RUNBOOK gotchas).

For changes outside `flink-runtime` / `flink-core`, swap the `-pl` target accordingly.

### Monitor code change

On Windows:
```bash
cd c:/Users/wagdy/git/flink-runtime-monitor
# edit, commit, push
mvn package -DskipTests -q     # produces the shaded JAR
scp target/flink-runtime-monitor-0.1-SNAPSHOT.jar \
    zs01:/mnt/labstore/aelmansoury/flink-runtime-monitor/
```

On zs01: restart the monitor process (no Flink restart needed).

### Fresh Flink build (full rebuild from scratch)

Rare — only after a Flink-version bump or accidental `target/` wipe. On zs01:
```bash
cd /mnt/labstore/aelmansoury/flinke2c/src
run_exp -m "FlinkE2C dist rebuild" -n 0 -t 0:45 -- \
    mvn package -pl flink-dist -am -DskipTests \
    -Denforcer.skip=true -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true -Drat.skip=true
# re-link build-target to the new built dist (path includes version)
ln -sfn $PWD/flink-dist/target/flink-2.2-SNAPSHOT-bin/flink-2.2-SNAPSHOT \
       /mnt/labstore/aelmansoury/flinke2c/build-target
# re-create the config symlinks
bash $PWD/scripts/deploy-configs.sh
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `start-cluster.sh` says "Permission denied (publickey)" for some workers | The zs01 internal pubkey isn't on that worker's `~/.ssh/authorized_keys` (homedirs are NOT shared NFS — only `/mnt/labstore`) | From Windows: `ssh <worker> "echo '<pubkey>' >> ~/.ssh/authorized_keys"` |
| Job stuck in `SCHEDULED` / `(unassigned)` | More slots requested than declared (`numberOfTaskSlots` < graphml `slots`) | Update one of the two to match, restart |
| Job stuck in `RESTARTING`, JM log says "Could not fulfill resource requirements" | Workload submitted with `flink run -p N` where N > slots-per-TM. FlinkE2C assigns one TM address **per operator**, so all N subtasks of an operator want slots on the same TM. Slot sharing can't fix this because slots can't span TMs. | Use `flink run -p 1` for now. Proper fix is per-subtask placement in `TopDownBottomUpExecutionGraphPlacement` (future work). |
| Capability-sort log line missing | `cluster.placement-method` not set to `BOTTOM_UP` (or `TOP_DOWN`) in config.yaml | Check `config.yaml`; remember it defaults to `DEFAULT` (vanilla Flink) |
| Dashboard shows 0 TMs | Workers crashed during start, or wrong hostnames in `workers` file | Check TM logs in `build-target/log/flink-aelmansoury-taskexecutor-*.out` |
| `git pull` complains about local changes | Someone (you, me, a script) edited a file under symlink target without committing. **The PlacementController mutates `cloud.graphml` at runtime** — those edits will look like local changes. | `cd flinke2c-conf && git diff` to see what; if it's monitor-induced shrinks, `git checkout -- cloud.graphml` to discard |
| Maven on zs01 fails enforcer check | Maven 3.8.7 vs Flink's pinned 3.8.6 | Always pass `-Denforcer.skip=true` (already in our build commands) |
| Cluster up but JM REST `:8081` unreachable from browser | Either: SSH tunnel not running, or wrong port | Re-run `ssh -N -L 8081:localhost:8081 zs01` |
| `ssh zs01 'java ... &'` exits with code 255 | Backgrounded Java process inherits SSH file descriptors → SSH can't close cleanly | Use an on-cluster wrapper script (e.g. `start-monitor.sh`) and redirect: `< /dev/null > log 2>&1 &` |
| `pkill -f <pattern>` kills your SSH session too | `-f` matches against the *full command line*, including the bash subshell SSH spawns to host that very `pkill` invocation. Even a narrow fully-qualified class name (e.g. `de.tuda.flink.monitor.E2cMonitor`) self-matches when wrapped in `ssh host '...'`. Inside a standalone shell script it's safe — the script's argv doesn't include the pattern. | Use pid-file kills (`kill $(cat ...pid)`) or port-based lookup (`ss -lptn "sport = :PORT"`). All launchers (`start-monitor.sh`, `start-sender.sh`, `start-viz.sh`) write pid files for exactly this reason. |
| Python f-string `f"...{x.get(\"key\")}..."` over bash fails with SyntaxError | Bash strips the escape backslashes inside the double-quoted string | Write the Python script to a temp file (`/tmp/foo.py`) and call `python3 /tmp/foo.py` |
| Dashboard "job graph unavailable — Failed to fetch" *(historical)* | The 2026-05-17 version of the dashboard fetched Flink REST cross-origin. CORS preflight blocked it without `Access-Control-Allow-Origin` | Since 2026-05-21 the dashboard reads from `status.json` only — no CORS dependency. If you still see this, you're on an old `web/index.html`; scp the current one in. |
| Nexmark Q5 (and other multi-vertex queries) fails with `"Not enough free slots for placement"` even at `parallelism.default=1` | FlinkE2C assigns one TM address per operator-vertex. Q5 has > 6 vertices, but we only have 6 TMs. | Pick a smaller-plan query (Q1, Q2, possibly Q3) for current demos. Proper fix is per-subtask placement in `TopDownBottomUpExecutionGraphPlacement` (future work). |

---

## Quick reference: most-used commands

```bash
# bring everything up (cluster + monitor + senders + nexmark + dashboard)
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/bootstrap.sh'

# open Flink JM dashboard
ssh -N -L 8081:localhost:8081 zs01    # then http://localhost:8081

# open the E2C monitor dashboard
start http://zs01.lab.tuda.systems:8082    # (or `ssh -N -L 8082:localhost:8082 zs01` then http://localhost:8082)

# submit examples
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/nexmark-submit.sh q1'
ssh zs01 'bash /mnt/labstore/aelmansoury/scripts/submit-workload.sh /mnt/labstore/aelmansoury/sql/hello.sql'

# deploy config change
# (1) on Windows: edit, commit, push
# (2) ssh zs01 'cd /mnt/labstore/aelmansoury/flinke2c/src && git pull'

# tail the JM log
ssh zs01 'tail -f /mnt/labstore/aelmansoury/flinke2c/build-target/log/*standalonesession*.log'

# tear down
ssh zs01 '/mnt/labstore/aelmansoury/flinke2c/build-target/bin/stop-cluster.sh'
```

---

## Related design docs

- **`STICKY-PLACEMENT-DESIGN.md`** (this repo, root) — **implemented 2026-06-08.** Sticky placement: a sidecar TSV (`cluster.placement.sticky-state-path`, defaults to a sibling of the graphml named `placement-state.tsv`) records the prior operator→host map; on each placement, operators are pinned to their previous host unless that host is now `excluded`. The `excluded` flag on compute nodes is read from `cloud.graphml`'s new `<key id="excluded">` declaration; the monitor's `PlacementController` toggles it via `GraphMlWriter.setExcluded(...)` instead of scaling `computeCapability`. See the design doc head for v1 implementation notes (single-writer-monitor model). Operator details live in `flink-runtime-monitor/RUNBOOK.md` → "Sticky placement + excluded flag".
