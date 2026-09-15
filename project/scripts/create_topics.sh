#!/usr/bin/env bash
set -e

BOOTSTRAP_SERVER=${1:-"localhost:9094"}

echo "Connecting to Kafka at ${BOOTSTRAP_SERVER}..."

# Wait for Kafka to become available
MAX_ATTEMPTS=30
ATTEMPT=1
until kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1 || [ $ATTEMPT -gt $MAX_ATTEMPTS ]; do
  echo "Waiting for Kafka to be ready (attempt $ATTEMPT/$MAX_ATTEMPTS)..."
  sleep 2
  ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
  echo "Error: Kafka did not become ready in time."
  exit 1
fi

echo "Creating topic: pulsestream.events (6 partitions, 7 days retention)..."
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic pulsestream.events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "Creating topic: pulsestream.events.dlq (3 partitions, 30 days retention)..."
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic pulsestream.events.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000

echo "Topics created successfully:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list
