# CEPless integration

Client-side integration of [CEPless](https://github.com/luthramanisha/CEPless) into FlinkE2C, used to
benchmark CEPless's serverless-operator offloading against FlinkE2C's external-runtime offloading on
the same Flink build.

CEPless was built against Flink 1.8. The code here is ported from a Flink 1.18 port to Flink 2.2
(`DataStream`/operator classes now live in `flink-runtime` instead of `flink-streaming-java`).

The comparison jobs, operators and experiment scripts are in the `flinke2c-edbt-2026/CEPless/` repo
(see its `FLINKE2C.md`).

## Layout

- Backend (unmodified, not Flink-specific): a Go NodeManager (one per node, Docker Swarm service)
  that starts/stops/updates operator containers, plus Redis or Infinispan as event transport.
  Lives in `flinke2c-edbt-2026/CEPless/` (`node-manager/`, `operators/`, `docker-compose.yml`).
- Client (this repo): connects Flink to the NodeManager and ships events to/from a deployed
  operator.

Client code, under `flink-runtime/src/main/java/org/apache/flink/streaming/api/`:

- `customoperators/UserDefinedOperatorInterface.java`: requests an operator from the NodeManager
  over HTTP, routes events through the configured `EventRepository`.
- `customoperators/CustomOperatorAddress.java`, `CustomOperatorDeployed.java`,
  `OperatorEventReceiver.java`: value/callback types.
- `customoperators/repository/`: `EventRepository` transports `RedisRepository` (batched Redis
  lists), `RedisPubSubRepository` (Redis pub/sub), `InfinispanRepository`.
- `operators/StreamCEPlessOperator.java`: offloads each element, emits what comes back.
- `operators/StreamCEPlessFilterOperator.java`: filter variant, keeps the original element and
  forwards it only if the operator responds `"true"`.
- `operators/StreamKMeansOperator.java`, `StreamBenchmarkOperator.java`,
  `StreamForwardOperator.java`: benchmark helpers (local k-means baseline, latency/throughput
  logger writing `eval.csv`/`throughput.csv` like CEPless's eval scripts, no-op pass-through).

New `DataStream<T>` methods (`datastream/DataStream.java`):

```java
stream.serverless("myOperator");         // offload to a CEPless-deployed operator
stream.serverlessFilter("myOperator");   // offload a filter predicate (keep/drop)
stream.kMeans();                         // local k-means baseline
stream.benchmark(eventRate);             // log per-event latency + throughput
stream.forwardOperator();                // no-op operator baseline
```

## Starting the backend

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

1. Start the backend.
2. Set on every TaskManager:
   - `NODE_MANAGER_HOST`: `host:port` of the local NodeManager.
   - `DB_TYPE`: `redis` (default), `redis-pubsub` or `infinispan`.
   - `REDIS_HOST` / `INFINISPAN_HOST`.
   - `OUT_BATCH_SIZE`, `IN_BATCH_SIZE`, `FLUSH_INTERVAL`, `BACK_OFF`: required, read by the
     repository constructors.
3. Call `.serverless("myOperator")` / `.serverlessFilter("myOperator")` in the job.

## Changes from the Flink 1.18 port

- Moved from `flink-streaming-java` to `flink-runtime`.
- No forced `chainingStrategy = ChainingStrategy.ALWAYS` (not settable on operator instances anymore).
- `forwardOperator()` instead of overloading `forward()`, which already means forward partitioning.
- `RedisPubSubRepository.listen()` no longer calls `Thread.currentThread().join()` (blocked `open()`
  with `DB_TYPE=redis-pubsub`).
- `StreamCEPlessOperator.operatorAddress` is `volatile` (written by the HTTP callback, read by the
  task thread).
- `UserDefinedOperatorInterface.getRepository()` no longer throws an NPE when `DB_TYPE` is unset.
- `StreamCEPlessOperator`/`StreamCEPlessFilterOperator` emit results via the `MailboxExecutor`, since
  `receivedEvent(...)` runs on the Redis receive thread.

## Known gaps

- Compiles against Flink 2.2 and round-trips locally against a NodeManager; not yet run end-to-end
  on the cluster.
