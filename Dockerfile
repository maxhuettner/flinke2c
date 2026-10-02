FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    prometheus-node-exporter \
    && rm -rf /var/lib/apt/lists/*

ENV FLINK_HOME=/opt/flink
ENV PATH="$FLINK_HOME/bin:$PATH"

WORKDIR $FLINK_HOME

# build-target is a symlink; Docker doesn't follow directory symlinks,
# so we reference the real path: flink-dist/target/flink-2.2-SNAPSHOT-bin/flink-2.2-SNAPSHOT
ARG FLINK_DIST=flink-dist/target/flink-2.2-SNAPSHOT-bin/flink-2.2-SNAPSHOT

# Copy build artifacts
COPY ${FLINK_DIST}/bin/        $FLINK_HOME/bin/
COPY ${FLINK_DIST}/lib/        $FLINK_HOME/lib/
COPY ${FLINK_DIST}/opt/        $FLINK_HOME/opt/
COPY ${FLINK_DIST}/plugins/    $FLINK_HOME/plugins/
COPY ${FLINK_DIST}/examples/   $FLINK_HOME/examples/

# Copy conf directory without config.yaml (supplied externally at runtime)
COPY ${FLINK_DIST}/conf/log4j.properties              $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/log4j-cli.properties          $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/log4j-console.properties      $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/log4j-session.properties      $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/logback.xml                   $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/logback-console.xml           $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/logback-session.xml           $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/masters                       $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/workers                       $FLINK_HOME/conf/
COPY ${FLINK_DIST}/conf/zoo.cfg                       $FLINK_HOME/conf/

COPY docker-entrypoint.sh $FLINK_HOME/bin/docker-entrypoint.sh

RUN chmod +x $FLINK_HOME/bin/*.sh \
    && mkdir -p $FLINK_HOME/log $FLINK_HOME/tmp

# Flink web UI / REST
EXPOSE 8081
# JobManager RPC
EXPOSE 6123
# TaskManager data / blob transfer ports
EXPOSE 6121 6122

# Usage: docker run ... <image> (jobmanager|taskmanager) /path/to/config.yaml [extra args]
ENTRYPOINT ["docker-entrypoint.sh"]
