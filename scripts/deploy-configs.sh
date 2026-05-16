#!/usr/bin/env bash
# Wire the FlinkE2C distribution's conf/ to read directly from this repo's
# flinke2c-conf/ via symlinks, so `git pull` is the deploy step for cluster
# configuration. Idempotent — safe to re-run after a fresh build.
#
# Run this on the JM host (zs01) after every fresh `mvn package -pl flink-dist`.
#
# Layout:
#   $SRC/flinke2c-conf/conf/{config.yaml,masters,workers}    ← source of truth
#   $SRC/flinke2c-conf/cloud.graphml                          ← source of truth
#   $FLINK_HOME/conf/{config.yaml,masters,workers,cloud.graphml}
#       → become symlinks back to the above
#
# Flink's own conf files (log4j*.properties, logback.xml, zoo.cfg) are left as
# actual files inside $FLINK_HOME/conf/.
set -euo pipefail

SRC=${SRC:-/mnt/labstore/aelmansoury/flinke2c/src}
FLINK_HOME=${FLINK_HOME:-/mnt/labstore/aelmansoury/flinke2c/build-target}

if [[ ! -d "$SRC/flinke2c-conf" ]]; then
    echo "ERROR: $SRC/flinke2c-conf not found — is SRC correct?" >&2
    exit 1
fi
if [[ ! -d "$FLINK_HOME/conf" ]]; then
    echo "ERROR: $FLINK_HOME/conf not found — has the dist been built?" >&2
    exit 1
fi

link() {
    local target=$1
    local linkname=$2
    if [[ ! -e "$target" ]]; then
        echo "WARN: target $target does not exist, skipping" >&2
        return
    fi
    rm -f "$linkname"
    ln -s "$target" "$linkname"
    echo "  linked $(basename "$linkname") -> $target"
}

link "$SRC/flinke2c-conf/conf/config.yaml" "$FLINK_HOME/conf/config.yaml"
link "$SRC/flinke2c-conf/conf/masters"     "$FLINK_HOME/conf/masters"
link "$SRC/flinke2c-conf/conf/workers"     "$FLINK_HOME/conf/workers"
link "$SRC/flinke2c-conf/cloud.graphml"    "$FLINK_HOME/conf/cloud.graphml"

echo "Done. Configs in $FLINK_HOME/conf/ now follow $SRC/flinke2c-conf/."
