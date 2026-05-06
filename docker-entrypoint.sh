#!/usr/bin/env bash
set -e

COMPONENT="${1:?Usage: docker-entrypoint.sh (jobmanager|taskmanager|sql-client) [extra args...]}"

case "$COMPONENT" in
    jobmanager|taskmanager|sql-client) ;;
    *)
        echo "ERROR: First argument must be 'jobmanager', 'taskmanager', or 'sql-client', got: $COMPONENT"
        exit 1
        ;;
esac

if [[ ! -d "/conf" ]]; then
    echo "ERROR: /conf directory not found. Mount your conf directory to /conf."
    exit 1
fi

if [[ ! -f "/conf/config.yaml" ]]; then
    echo "ERROR: /conf/config.yaml not found."
    exit 1
fi

# Symlink every file in /conf into $FLINK_HOME/conf/
for f in /conf/*; do
    [[ -e "$f" ]] || continue
    ln -sf "$f" "$FLINK_HOME/conf/$(basename "$f")"
done

if [[ "${CAPSYS_ENABLE_NODE_EXPORTER:-true}" == "true" ]]; then
    /usr/bin/prometheus-node-exporter \
        --web.listen-address="0.0.0.0:9100" &
fi

echo "Starting Flink $COMPONENT"

case "$COMPONENT" in
    jobmanager|taskmanager)
        exec "$FLINK_HOME/bin/${COMPONENT}.sh" start-foreground "${@:2}"
        ;;
    sql-client)
        exec "$FLINK_HOME/bin/sql-client.sh" "${@:2}"
        ;;
esac
