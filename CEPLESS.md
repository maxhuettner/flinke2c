# CEPless integration

We added a client-side integration of
[CEPless](https://github.com/luthramanisha/CEPless) into FlinkE2C, so that CEPless's
serverless-operator offloading can be benchmarked against FlinkE2C's own external-runtime
offloading on the same Flink build.

CEPless was originally built against Flink 1.8. 
Flink 2.x's module layout (the `DataStream`/operator classes moved from `flink-streaming-java`
into `flink-runtime`) and to a few API changes described under "Adaptations" below.

## What CEPless is, and what lives where

CEPless splits into two parts:

- **A CEP-engine-agnostic backend**: a Go *NodeManager* (one per cluster node, deployed as a
  global Docker Swarm service) that starts/stops/updates user-defined operators as containers,
  plus an event transport (Redis or Infinispan) that operators use to exchange events with the
  engine. This part does not depend on the Flink version at all and is reused unmodified. It lives
  in `flinke2c-edbt-2026/CEPless/` (`node-manager/`, `operators/`, `docker-compose.yml`).
- **A per-engine client**: the piece that lets the stream processor talk to the NodeManager and
  ship events to/from a deployed operator. This is what's ported into this repository, under
  `org.apache.flink.streaming.api.customoperators` and the new `Stream*Operator` classes below.

## What was added

All new code lives in `flink-runtime/src/main/java/org/apache/flink/streaming/api/`:

- `customoperators/UserDefinedOperatorInterface.java` — requests an operator from the NodeManager
  over HTTP and routes events through the configured `EventRepository`.
- `customoperators/CustomOperatorAddress.java`, `CustomOperatorDeployed.java`,
  `OperatorEventReceiver.java` — small value/callback types used by the interface above.
- `customoperators/repository/` — three interchangeable `EventRepository` transports:
  `RedisRepository` (batched Redis lists), `RedisPubSubRepository` (Redis pub/sub), and
  `InfinispanRepository` (Infinispan remote cache with entry-created/modified listeners).
- `operators/StreamCEPlessOperator.java` — the actual offloading operator: on `open()` it requests
  an operator from the NodeManager by name, then forwards every incoming element to it and emits
  whatever comes back.
- `operators/StreamCEPlessFilterOperator.java` — the filter-predicate variant: keeps the original
  element and forwards it only when the deployed operator responds `"true"` for it (dropping it
  otherwise), instead of replacing it. This is the CEPless-side equivalent of a SQL `WHERE
  predicate(...)` externalized via FlinkE2C's own external-runtime mechanism. See "Response
  correlation" below for how it matches responses back to requests.
- `operators/StreamKMeansOperator.java`, `StreamBenchmarkOperator.java`,
  `StreamForwardOperator.java` — workload/measurement operators used to build comparable
  benchmark jobs: an in-JVM k-means baseline, a per-event latency/throughput logger (appends to
  `eval.csv`/`throughput.csv`, same format as the original CEPless eval scripts), and a zero-logic
  pass-through operator used as a graph-overhead baseline.

And four new convenience methods on `DataStream<T>`
(`flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java`):

```java
stream.serverless("myOperator");         // offload to a CEPless-deployed operator
stream.serverlessFilter("myOperator");   // offload a filter predicate: keep/drop, don't replace
stream.kMeans();                         // run k-means locally, as a baseline
stream.benchmark(eventRate);             // log per-event latency + throughput
stream.forwardOperator();                // no-op operator, as a graph-overhead baseline
```

## Starting the CEPless infrastructure

The NodeManager lives in `flinke2c-edbt-2026/CEPless/node-manager/` (Go); it and the Redis/Infinispan
event store are unmodified and version-independent, and must be reachable from every TaskManager.
From `flinke2c-edbt-2026/CEPless/`:

```bash
cd node-manager
cp config.example.yaml config.yaml   # fill in a Docker Hub username/password — NodeManager
                                       # pulls operator images from docker.io/<user>/<operatorName>
docker build -t node-manager .

cd ..
docker network create node-manager-net
docker-compose up -d                  # starts redis + nodeManager (ports 6379, 25003)
docker logs -f <project>_nodeManager_1
```

Before requesting an operator by name, its image must exist under that name in the same registry
account: `cd operators/<template> && ./build.sh <docker-hub-user> <operatorName>` builds and
pushes it (see "Externalizing a filter predicate" below for a worked example).

## Running it

1. Start the CEPless infrastructure as above.
2. Set these environment variables on every TaskManager process:
   - `NODE_MANAGER_HOST` — `host:port` of the local NodeManager.
   - `DB_TYPE` — `redis` (default, batched list-based), `redis-pubsub`, or `infinispan`.
   - `REDIS_HOST` (for `redis`/`redis-pubsub`) or `INFINISPAN_HOST` (for `infinispan`).
   - `OUT_BATCH_SIZE`, `IN_BATCH_SIZE`, `FLUSH_INTERVAL`, `BACK_OFF` — batching/backoff tuning for
     `RedisRepository`/`InfinispanRepository` (required; the constructors read them eagerly).
3. Call `.serverless("myOperator")` / `.serverlessFilter("myOperator")` at the point in your job
   graph where FlinkE2C would otherwise offload to its own external runtime, using an operator name
   that the NodeManager has an image registered for under `operators/`.

## CEPless has no "drop" primitive

CEPless's reference operator protocol (`EventManager.process(item)` → `operator.process(item)` →
`send(result)`) always produces exactly one response per request — there is no way for an operator
to emit nothing for an input. Every reference example confirms this (`forward-java` echoes,
`k-means-java` returns computed means, and `java-fraud-detection` — the closest thing to a filter —
*prepends* a `"0"`/`"1"` flag to the unmodified row rather than dropping it). So "filtering via
CEPless" always means: CEPless annotates every row with a verdict, and something else (Flink) does
the actual dropping. Two ways to build that:

- **`.serverless(name)` + a plain local `.filter()`** (what `Q1CeplessPriceGreaterThanJob` uses):
  the operator returns `"true,"`/`"false," + original row`, unchanged CSV field-for-field, and a
  normal Flink `.filter(row -> row.startsWith("true,"))` right after does the drop. No
  request/response correlation needed, since `.serverless`'s `StreamCEPlessOperator` just forwards
  whatever comes back — this is the closer fit to how CEPless's own examples are built.
- **`.serverlessFilter(name)`** (`StreamCEPlessFilterOperator`): a stateful operator that tracks
  in-flight requests and drops non-matching rows itself, so the caller doesn't need a separate
  `.filter()` step. CEPless's Redis-backed transport batches sends/receives
  (`OUT_BATCH_SIZE`/`IN_BATCH_SIZE`) with no guarantee responses arrive in request order, so this
  prefixes every outgoing payload with a sequence number (`"<seq>|<value.toString()>"`) and expects
  it echoed back (`"<seq>|true"` / `"<seq>|false"`) rather than assuming order — an earlier version
  assumed order and silently dropped results under load once responses stopped lining up 1:1 with
  sends. Available if you want single-call filter ergonomics elsewhere; any operator used this way
  needs to echo the sequence prefix.

Either way, CEPless can't skip the round trip for a row that ends up dropped — every row is always
sent and always gets a response, unlike a hypothetical external-runtime `FILTER` kind that could
suppress the response for non-matching rows. That's a structural property of CEPless's protocol,
not an implementation choice here, worth noting for any fairness discussion in the comparison.

## Externalizing a filter predicate (e.g. PriceGreaterThan)

FlinkE2C's own external-runtime mechanism (`table.exec.external-runtime.function-class` /
`.conf`) hooks directly into the Table planner's `Calc` translation (see
`CommonExecCalc.createExternalRuntimeChain`) and ships rows to a process speaking its own TCP/RDMA
binary protocol. CEPless has no equivalent planner hook — only the `DataStream` methods above — and
a CEPless operator doesn't run your `ScalarFunction` class at all; it's a separate Docker image that
independently reimplements the same logic and speaks CEPless's plain-string Redis/Infinispan
protocol. So getting equivalent end-to-end behavior (same filter, same source/sink) out of CEPless
takes two pieces instead of a SQL option:

1. **A CEPless operator implementing the predicate.** See
   `flinke2c-edbt-2026/CEPless/operators/price-greater-than-java/`, built from the
   `java-fraud-detection` template: `operator/Operator.java` parses the price field (index 2, i.e.
   `auction,bidder,price,...`, overridable via the `PRICE_FIELD_INDEX` env var) and prepends
   `"true,"`/`"false,"` to the unmodified row depending on whether it exceeds a `PRICE_THRESHOLD`
   env var (default `1000`). Build and push it with `./build.sh <docker-hub-user>
   price-greater-than`.
2. **`Q1CeplessPriceGreaterThanJob`** (`flinke2c-edbt-2026/CEPless/flink-e2c-job/`) — the DataStream
   equivalent of the nexmark_q1 SQL query. There's no CEPless equivalent of a `tcp-source`/
   `tcp-sink` *table* connector, so this reads/writes `RowData` directly using
   `BinaryTcpSourceFunction`/`BinaryTcpSink` — the same `TcpBinaryCodec` wire format your real
   `bids`/`nexmark_q1` connectors use (extracted from `~/dev/work/nexmark/my-flink-udfs`'s
   `TcpTableSource`/`TcpTableSink`, dropping their `DynamicTableSource`/`DynamicTableSink` wrapper
   since there's no Table/SQL registration here). Each `Bid` row is converted to a
   `auction,bidder,price,dateTime,extra,latency_ts` CSV string, sent through
   `.serverless("price-greater-than")`, filtered on the `"true,"` prefix, stripped back to plain
   CSV, and parsed into `RowData` for the sink — matching the field layout and `DECIMAL(23,3)`
   scaling `nexmark/table-api-queries/src/main/java/Q1Flinke2cParallelStreamJob.java` already uses
   for the equivalent local-imputation comparison job. Build with `mvn package` in
   `flink-e2c-job/`; run with `flink run -c
   org.example.flinke2c.Q1CeplessPriceGreaterThanJob flinke2c-cepless-job-1.0-SNAPSHOT.jar
   --bids.host <host> --bids.port <port> --sink.host <host> --sink.port <port> --operator
   price-greater-than`.

## Live operator updates (selectivity-change experiment)

NodeManager can swap the deployed operator while a job keeps running, via
`OPERATOR_UPDATE_NAME`/`OPERATOR_UPDATE_TIMEOUT` in `docker-compose.yml` (both default
disabled/`0`, so normal runs aren't disrupted). `OPERATOR_UPDATE_TIMEOUT` seconds after an operator
is deployed, NodeManager starts `OPERATOR_UPDATE_NAME`'s container on the *same* `addrIn`/`addrOut`
queues, waits 5s, then stops the original — the running Flink client never sees the swap, since its
queue addresses never change. This is CEPless's own equivalent of
`streaming-exp-management/flink/run_q1f_update_flinke2c.sh`'s live JAR/`.so` swap under a running
job, just performed server-side by NodeManager instead of by the experiment script.

`operators/price-greater-than-updated-java/` is the "updated" counterpart to
`price-greater-than-java`: identical code, but its Dockerfile bakes `PRICE_THRESHOLD=100000` (vs.
the original's `1000`) via `ENV`, matching
`nexmark/flinke2c/src/updated/java/org/example/flinke2c/PriceGreaterThan.java`'s threshold exactly
— the same selectivity change as the reference experiment. Built and pushed as
`maxhue/price-greater-than-updated`. NodeManager's deployment mechanism doesn't support
per-deployment env var overrides, which is why this needs its own image rather than a
`PRICE_THRESHOLD` override at request time.

`streaming-exp-management/flink/run_q1f_update_cepless.sh` drives the experiment: it configures
NodeManager once (`OPERATOR_UPDATE_NAME=price-greater-than-updated
OPERATOR_UPDATE_TIMEOUT=<seconds> docker-compose up -d`, default 5s), then submits
`Q1CeplessPriceGreaterThanJob` `--repeat` times — no per-iteration artifact-copy step needed, since
NodeManager performs the swap automatically on each run. Restores `OPERATOR_UPDATE_TIMEOUT=0` on
exit unless `--keep-node-manager-config` is passed. Host/path defaults in that script (`zs01`/`zs02`,
`/home/mhuttner/...`) are guesses based on earlier conventions in this conversation and this
script hasn't been run end-to-end — check them before trusting a first run.

Known gap: `extra` is assumed comma-free when building/parsing that CSV round trip — a literal
comma in it would misalign the fields after it.

## Adaptations from the Flink 1.18 port

- Moved from `flink-streaming-java` to `flink-runtime`, following where `DataStream` and the
  `StreamOperator` hierarchy live in this Flink version.
- Dropped the forced `chainingStrategy = ChainingStrategy.ALWAYS` in the constructors: operator
  instances no longer expose that field directly in this Flink version (chaining is now set on the
  transformation/operator factory instead), and the default chaining behavior is sufficient here.
- Added `forwardOperator()` instead of overriding `forward()`: this codebase's `forward()` already
  has real (and different) meaning — it selects forward partitioning, not a pass-through operator
  — so overloading it purely by return type (as the 1.18 port did) isn't valid Java and would have
  been a confusing collision anyway.
- `RedisPubSubRepository.listen()` no longer calls `Thread.currentThread().join()`. That call
  blocked forever on the operator's `open()` thread — which runs `listen()` synchronously — for no
  benefit, since Lettuce's pub/sub listener already delivers messages asynchronously; it would have
  deadlocked task initialization whenever `DB_TYPE=redis-pubsub`.
- `StreamCEPlessOperator.operatorAddress` is now `volatile`: it's written from the NodeManager
  HTTP-response callback thread and read from the task thread in `processElement`, so it needs
  proper cross-thread visibility.
- `UserDefinedOperatorInterface.getRepository()` no longer throws an NPE when `DB_TYPE` is unset.
- `StreamCEPlessOperator`/`StreamCEPlessFilterOperator` now implement `YieldingOperator` and defer
  their `collector.collect(...)` calls to the injected `MailboxExecutor` instead of calling it
  directly from `receivedEvent(...)`. That method runs on CEPless's Redis receive thread, not the
  task's own mailbox thread, and calling `collect` from the wrong thread races with the task
  thread's own writes to the same output/network buffers — observed in practice as `Corrupt
  stream, found tag: ...` deserialization failures on a downstream task (with disabled operator
  chaining, every operator is its own task connected via Flink's internal network stack, so this
  is a real inter-task race, not just a theoretical concern), reliably right as a bounded job's
  source finishes and both threads are touching the output around the same time. The original
  1.18 port had the same unguarded direct call; it just never surfaced visibly there.

## Known gaps

- This integration has been verified to compile against this repository's Flink 2.2 base; it has
  not yet been run end-to-end against a live NodeManager from this repo. Validate connectivity and
  the event round-trip before trusting benchmark numbers from it.
- The k-means/benchmark/forward operators mirror the workload used for the EDBT evaluation; adjust
  or replace them for other comparison workloads.
