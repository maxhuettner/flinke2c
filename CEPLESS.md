# CEPless integration

Client-side integration of [CEPless](https://github.com/luthramanisha/CEPless) into FlinkE2C, so
CEPless's serverless-operator offloading can be benchmarked against FlinkE2C's own external-runtime
offloading on the same Flink build.

CEPless was built against Flink 1.8. Porting it to Flink 2.2 mainly meant following the module
move (`DataStream`/operator classes moved from `flink-streaming-java` to `flink-runtime`) plus the
API changes listed under "Adaptations" below.

The comparison jobs, operators, and experiment scripts built on top of this live in the
`flinke2c-edbt-2026/CEPless/` repo — see its `FLINKE2C.md`.

## Layout

CEPless has two parts:

- **Backend** (unmodified, not Flink-specific): a Go NodeManager (one per node, deployed as a
  Docker Swarm service) that starts/stops/updates operator containers, plus Redis or Infinispan as
  the event transport. Lives in `flinke2c-edbt-2026/CEPless/` (`node-manager/`, `operators/`,
  `docker-compose.yml`).
- **Client** (what's new here): lets Flink talk to the NodeManager and ship events to/from a
  deployed operator. Lives under `org.apache.flink.streaming.api.customoperators` and the new
  `Stream*Operator` classes below.

## What was added

All new code is under `flink-runtime/src/main/java/org/apache/flink/streaming/api/`:

- `customoperators/UserDefinedOperatorInterface.java` — requests an operator from the NodeManager
  over HTTP, routes events through the configured `EventRepository`.
- `customoperators/CustomOperatorAddress.java`, `CustomOperatorDeployed.java`,
  `OperatorEventReceiver.java` — value/callback types for the above.
- `customoperators/repository/` — three `EventRepository` transports: `RedisRepository` (batched
  Redis lists), `RedisPubSubRepository` (Redis pub/sub), `InfinispanRepository`.
- `operators/StreamCEPlessOperator.java` — offloads each element to a deployed operator, emits
  whatever comes back.
- `operators/StreamCEPlessFilterOperator.java` — filter variant: keeps the original element,
  forwards it only if the operator responds `"true"`. See `FLINKE2C.md`'s "No drop primitive".
- `operators/StreamKMeansOperator.java`, `StreamBenchmarkOperator.java`,
  `StreamForwardOperator.java` — benchmark scaffolding: in-JVM k-means baseline, a
  latency/throughput logger (`eval.csv`/`throughput.csv`, same format as CEPless's own eval
  scripts), a no-op pass-through for graph-overhead measurement.

Four new `DataStream<T>` methods (`datastream/DataStream.java`):

```java
stream.serverless("myOperator");         // offload to a CEPless-deployed operator
stream.serverlessFilter("myOperator");   // offload a filter predicate: keep/drop, don't replace
stream.kMeans();                         // run k-means locally, as a baseline
stream.benchmark(eventRate);             // log per-event latency + throughput
stream.forwardOperator();                // no-op operator, as a graph-overhead baseline
```

## Starting it

```bash
cd flinke2c-edbt-2026/CEPless/node-manager
cp config.example.yaml config.yaml   # Docker Hub username/password - NodeManager pulls operator
                                       # images from docker.io/<user>/<operatorName>
docker build -t node-manager .

cd ..
docker network create node-manager-net
docker-compose up -d                  # redis + nodeManager (ports 6379, 25003)
docker logs -f <project>_nodeManager_1
```

Build and push an operator image before requesting it by name:
`cd operators/<template> && ./build.sh <docker-hub-user> <operatorName>`.

## Running a job

1. Start the infrastructure above.
2. Set on every TaskManager:
   - `NODE_MANAGER_HOST` — `host:port` of the local NodeManager.
   - `DB_TYPE` — `redis` (default), `redis-pubsub`, or `infinispan`.
   - `REDIS_HOST` / `INFINISPAN_HOST`.
   - `OUT_BATCH_SIZE`, `IN_BATCH_SIZE`, `FLUSH_INTERVAL`, `BACK_OFF` — required, read eagerly by
     the repository constructors.
3. Call `.serverless("myOperator")` / `.serverlessFilter("myOperator")` where FlinkE2C would
   otherwise call its own external runtime. See `flinke2c-edbt-2026/CEPless/FLINKE2C.md` for the
   three comparison jobs built this way.

## Adaptations from the Flink 1.18 port

- Moved from `flink-streaming-java` to `flink-runtime`, following `DataStream`/`StreamOperator` in
  this version.
- Dropped the forced `chainingStrategy = ChainingStrategy.ALWAYS`: that field isn't exposed on
  operator instances anymore (chaining moved to the transformation/factory), default behavior is
  fine here.
- `forwardOperator()` instead of overriding `forward()` — `forward()` already means forward
  partitioning in this codebase, overloading by return type isn't valid Java anyway.
- `RedisPubSubRepository.listen()` no longer calls `Thread.currentThread().join()`. It blocked
  `open()` forever for no reason — Lettuce's pub/sub listener is already async — and would deadlock
  task init under `DB_TYPE=redis-pubsub`.
- `StreamCEPlessOperator.operatorAddress` is `volatile`: written from the HTTP-response callback,
  read from the task thread in `processElement`.
- `UserDefinedOperatorInterface.getRepository()` no longer NPEs when `DB_TYPE` is unset.
- `StreamCEPlessOperator`/`StreamCEPlessFilterOperator` defer `collector.collect(...)` to the
  injected `MailboxExecutor` instead of calling it directly from `receivedEvent(...)`, which runs
  on CEPless's Redis receive thread, not the task's mailbox thread. Calling `collect` from the
  wrong thread races with the task thread's own output writes — seen as `Corrupt stream, found
  tag: ...` on a downstream task, reliably right as a bounded job's source finishes. The 1.18 port
  had the same unguarded call; it just never surfaced there.

## Known gaps

- Verified to compile against this repo's Flink 2.2 base and to round-trip locally against a
  NodeManager; not yet run end-to-end on the real cluster. Validate connectivity and throughput
  before trusting benchmark numbers.
