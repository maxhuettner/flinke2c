#!/bin/bash

mkdir -p libs/

"$(dirname "$0")/../sync-flink-libs.sh"

./gradlew clean build
