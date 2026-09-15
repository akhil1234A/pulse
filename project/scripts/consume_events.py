"""Interactive CLI viewer to watch live events arriving on Kafka topics."""

import argparse
from pathlib import Path
import sys
import time

# Ensure project root is in sys.path when running script directly
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.common.config import get_settings
from src.common.kafka_utils import create_consumer
from src.common.models import PulseStreamEvent


def view_events(topic: str = "pulsestream.events", max_events: int = 0) -> None:
    settings = get_settings()
    group_id = f"cli-viewer-{int(time.time())}"

    print(f"Connecting to Kafka at {settings.kafka_bootstrap_servers}...")
    print(f"Subscribed to topic '{topic}'. Listening for live events (Ctrl+C to stop)...\n")

    consumer = create_consumer(
        group_id=group_id,
        topics=[topic],
        auto_offset_reset="latest",
    )

    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            count += 1
            if topic.endswith(".dlq"):
                print(f"[{count}] DLQ MSG | key: {msg.key().decode('utf-8', errors='ignore')} | value: {msg.value().decode('utf-8', errors='ignore')[:150]}...")
            else:
                event = PulseStreamEvent.from_avro_bytes(msg.value())
                print(
                    f"[{count:04d}] [{event.source.value:<9}] {event.event_type.value:<16} | "
                    f"community: {str(event.community_id):<15} | user: {str(event.user_id):<20} | "
                    f"id: {event.event_id[:8]}"
                )

            if max_events > 0 and count >= max_events:
                break

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Watch live PulseStream events from Kafka.")
    parser.add_argument(
        "--topic",
        default="pulsestream.events",
        help="Kafka topic to consume from (default: pulsestream.events)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Stop after N events (default: 0 = continuous)",
    )
    args = parser.parse_args()
    view_events(topic=args.topic, max_events=args.count)
