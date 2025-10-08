#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STREAMING_JAR_SRC="$ROOT_DIR/../flink/lib/flink-streaming-java-1.18.1.jar"
if [[ ! -f "$STREAMING_JAR_SRC" ]]; then
  STREAMING_JAR_SRC="$ROOT_DIR/../flink/flink-streaming-java/target/flink-streaming-java-1.18-SNAPSHOT.jar"
fi
DIST_PATCH_CLASS_DIR="$ROOT_DIR/../flink/flink-streaming-java/target/classes"
TABLE_API_JAR_SRC="$ROOT_DIR/../flink/lib/flink-table-api-java-uber-1.18.1.jar"
if [[ ! -f "$TABLE_API_JAR_SRC" ]]; then
  TABLE_API_JAR_SRC="$ROOT_DIR/../flink/build-target/lib/flink-table-api-java-uber-1.18-SNAPSHOT.jar"
fi

TABLE_RUNTIME_JAR_SRC="$ROOT_DIR/../flink/lib/flink-table-runtime-1.18.1.jar"
if [[ ! -f "$TABLE_RUNTIME_JAR_SRC" ]]; then
  TABLE_RUNTIME_JAR_SRC="$ROOT_DIR/../flink/build-target/lib/flink-table-runtime-1.18-SNAPSHOT.jar"
fi

declare -A MAVEN_COORDS=(
  [httpclient]="org/apache/httpcomponents/httpclient:httpclient"
  [httpcore]="org/apache/httpcomponents/httpcore:httpcore"
  [commons-codec]="commons-codec/commons-codec:commons-codec"
  [commons-logging]="commons-logging/commons-logging:commons-logging"
  [gson]="com/google/code/gson/gson:gson"
  [lettuce-core]="io/lettuce/lettuce-core:lettuce-core"
  [netty-common]="io/netty/netty-common:netty-common"
  [netty-buffer]="io/netty/netty-buffer:netty-buffer"
  [netty-codec]="io/netty/netty-codec:netty-codec"
  [netty-handler]="io/netty/netty-handler:netty-handler"
  [netty-transport]="io/netty/netty-transport:netty-transport"
  [netty-resolver]="io/netty/netty-resolver:netty-resolver"
  [reactor-core]="io/projectreactor/reactor-core:reactor-core"
  [reactive-streams]="org/reactivestreams/reactive-streams:reactive-streams"
  [scala-library]="org/scala-lang/scala-library/2.12.15:scala-library"
  [scala-java8-compat]="org/scala-lang/modules/scala-java8-compat_2.12/1.0.2:scala-java8-compat_2.12"
  [scala-reflect]="org/scala-lang/scala-reflect/2.12.15:scala-reflect"
)

DEST_DIRS=(
  "$ROOT_DIR/flink-test/libs"
  "$ROOT_DIR/../flink/build-target/lib"
)

echo "Ensuring destination directories exist…"
for dir in "${DEST_DIRS[@]}"; do
  mkdir -p "$dir"
done

echo "Copying patched flink-streaming-java jar to destinations…"
if [[ ! -f "$STREAMING_JAR_SRC" ]]; then
  echo "Missing patched jar: $STREAMING_JAR_SRC" >&2
  exit 1
fi
for dir in "${DEST_DIRS[@]}"; do
  if [[ ! -w "$dir" ]]; then
    echo "Skipping streaming/table jar copy to $dir (not writable)" >&2
    continue
  fi

  if [[ "$dir" != "$ROOT_DIR/flink-test/libs" ]]; then
    rm -f "$dir/$(basename "$STREAMING_JAR_SRC")" 2>/dev/null || true
    cp "$STREAMING_JAR_SRC" "$dir/" 2>/dev/null || true
  fi

  rm -f "$dir/$(basename "$TABLE_API_JAR_SRC")" 2>/dev/null || true
  rm -f "$dir/$(basename "$TABLE_RUNTIME_JAR_SRC")" 2>/dev/null || true
  cp "$TABLE_API_JAR_SRC" "$dir/" 2>/dev/null || true
  cp "$TABLE_RUNTIME_JAR_SRC" "$dir/" 2>/dev/null || true
done

echo "Copying third-party dependencies from local Maven repository…"
for name in "${!MAVEN_COORDS[@]}"; do
  IFS=":" read -r rel_path prefix <<< "${MAVEN_COORDS[$name]}"
  search_dir="$HOME/.m2/repository/$rel_path"
  if [[ ! -d "$search_dir" ]]; then
    echo "Dependency directory missing for $name ($search_dir)" >&2
    exit 1
  fi

  jar_path=$(find "$search_dir" -maxdepth 5 -type f -name "${prefix}-*.jar" \
    ! -name "*-sources.jar" ! -name "*-javadoc.jar" | sort -V | tail -n1)

  if [[ -z "$jar_path" ]]; then
    echo "No jar found for $name under $search_dir" >&2
    exit 1
  fi

  base_name=$(basename "$jar_path")
  for dir in "${DEST_DIRS[@]}"; do
    if [[ "$dir" == "$ROOT_DIR/flink-test/libs" ]]; then
      continue
    fi
    if [[ ! -w "$dir" ]]; then
      echo "Skipping copy of $base_name to $dir (not writable)" >&2
      continue
    fi
    rm -f "$dir/${prefix}-"*.jar 2>/dev/null || true
    cp "$jar_path" "$dir/$base_name" 2>/dev/null || true
  done
done

echo "Refreshing flink-dist jar with patched DataStream class…"
DIST_JAR_PATHS=(
  "$ROOT_DIR/../flink/build-target/lib/flink-dist-1.18-SNAPSHOT.jar"
)

for dist in "${DIST_JAR_PATHS[@]}"; do
  if [[ ! -f "$dist" ]]; then
    echo "Distribution jar missing: $dist" >&2
    exit 1
  fi
  if [[ ! -w "$dist" ]]; then
    echo "Skipping update of $dist (not writable)" >&2
    continue
  fi
  jar uf "$dist" -C "$DIST_PATCH_CLASS_DIR" org/apache/flink/streaming/api/datastream/DataStream.class 2>/dev/null || true
done

echo "Copying patched flink-dist jar into flink-test libs…"
if [[ -w "$ROOT_DIR/flink-test/libs" ]]; then
  cp "$ROOT_DIR/../flink/build-target/lib/flink-dist-1.18-SNAPSHOT.jar" "$ROOT_DIR/flink-test/libs/" 2>/dev/null || true
fi

echo "Dependency sync complete. Restart Flink services to pick up updated jars."
