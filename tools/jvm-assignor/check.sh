#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA44_LIBS:?Set KAFKA44_LIBS to the released Kafka 4.4 libs directory}"
cd "$(dirname "$0")"
mkdir -p build/worker build/fixture
javac -cp "$KAFKA44_LIBS/*" -d build/worker AssignorWorker.java
javac -cp "$KAFKA44_LIBS/*" -d build/fixture fixtures/Fixture.java
jar cf build/fixture.jar -C build/fixture Fixture.class
python3 -m unittest discover -s . -v
