"""Smoke test script for PulseStream Phase 1 Foundation.

Verifies:
1. Redis SET/GET and dedup operations.
2. PostgreSQL INSERT and SELECT on partitioned events table.
3. Kafka produce 100 Avro-serialized events and consume/deserialize roundtrip.
"""

import asyncio
import sys
import time
import uuid

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings
from src.common.db import batch_insert_events, close_db_pool, create_db_pool
from src.common.kafka_utils import create_consumer, create_producer, produce_event
from src.common.models import EventSource, EventType, PulseStreamEvent
from src.common.redis_utils import check_and_set_dedup_async, get_async_redis_client


async def verify_redis(redis_url: str) -> bool:
    print("\n[1/3] Testing Redis...")
    client = None
    try:
        client = get_async_redis_client(redis_url)
        test_key = f"ps:smoke:test:{uuid.uuid4().hex[:8]}"
        await client.set(test_key, "ok", ex=10)
        val = await client.get(test_key)
        await client.delete(test_key)

        # Test dedup helper
        event_id = str(uuid.uuid4())
        is_dupe_1 = await check_and_set_dedup_async(client, event_id, ttl_seconds=10)
        is_dupe_2 = await check_and_set_dedup_async(client, event_id, ttl_seconds=10)
        await client.delete(f"ps:dedup:{event_id}")

        if val == "ok" and not is_dupe_1 and is_dupe_2:
            print("  [PASS] Redis SET/GET and dedup verified successfully.")
            return True
        else:
            print(f"  [FAIL] Redis unexpected state (val={val}, is_dupe_1={is_dupe_1}, is_dupe_2={is_dupe_2})")
            return False
    except Exception as e:
        print(f"  [FAIL] Redis connection error: {e}")
        return False
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass


async def verify_postgres(dsn: str) -> bool:
    print("\n[2/3] Testing PostgreSQL...")
    pool = None
    try:
        pool = await create_db_pool(dsn=dsn, min_size=1, max_size=2)
        sample_event = PulseStreamEvent(
            event_type=EventType.POST_CREATED,
            source=EventSource.SIMULATOR,
            user_id="smoke_user",
            community_id="smoke_community",
            payload={"test": True, "phase": 1},
        )

        # Batch insert using COPY
        inserted = await batch_insert_events(pool, [sample_event])
        if inserted != 1:
            print(f"  [FAIL] PostgreSQL insert failed: expected 1 row, got {inserted}")
            return False

        # Query back
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, event_type, source, user_id, community_id FROM events WHERE event_id = $1",
                uuid.UUID(sample_event.event_id),
            )
            # Clean up test row
            await conn.execute("DELETE FROM events WHERE event_id = $1", uuid.UUID(sample_event.event_id))

        if row and str(row["event_id"]) == sample_event.event_id:
            print("  [PASS] PostgreSQL COPY insert and SELECT verified successfully.")
            return True
        else:
            print(f"  [FAIL] PostgreSQL verify failed: row query returned {row}")
            return False
    except Exception as e:
        print(f"  [FAIL] PostgreSQL error: {e}")
        return False
    finally:
        if pool is not None:
            try:
                await close_db_pool(pool)
            except Exception:
                pass


def ensure_kafka_topic(bootstrap_servers: str, topic: str, num_partitions: int = 6) -> None:
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin_client.list_topics(timeout=5)
    if topic not in metadata.topics:
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                pass


def verify_kafka(bootstrap_servers: str, topic: str, count: int = 100) -> bool:
    print(f"\n[3/3] Testing Kafka (producing and consuming {count} Avro events)...")
    try:
        ensure_kafka_topic(bootstrap_servers, topic)
        producer = create_producer(bootstrap_servers=bootstrap_servers)

        delivered_count = 0

        def on_delivery(err, msg):
            nonlocal delivered_count
            if err is None:
                delivered_count += 1

        sent_ids = set()
        print(f"  -> Producing {count} events to topic '{topic}'...")
        for i in range(count):
            event = PulseStreamEvent(
                event_type=EventType.POST_CREATED if i % 2 == 0 else EventType.COMMENT_CREATED,
                source=EventSource.SIMULATOR,
                user_id=f"smoke_user_{i % 10}",
                community_id=f"smoke_comm_{i % 5}",
                payload={"index": i, "timestamp_ms": int(time.time() * 1000)},
            )
            sent_ids.add(event.event_id)
            produce_event(producer, topic, event, on_delivery=on_delivery)

        producer.flush(timeout=15)
        print(f"  -> Delivered {delivered_count}/{count} messages to Kafka.")

        if delivered_count != count:
            print(f"  [FAIL] Failed to deliver all messages (delivered {delivered_count}/{count})")
            return False

        # Consume back
        group_id = f"smoke-consumer-{uuid.uuid4().hex[:6]}"
        consumer = create_consumer(
            group_id=group_id,
            topics=[topic],
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
        )

        print("  -> Consuming and deserializing events...")
        consumed_ids = set()
        start_time = time.time()
        timeout_seconds = 20

        while len(consumed_ids) < count and (time.time() - start_time) < timeout_seconds:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            deserialized_event = PulseStreamEvent.from_avro_bytes(msg.value())
            if deserialized_event.event_id in sent_ids:
                consumed_ids.add(deserialized_event.event_id)

        consumer.close()

        if len(consumed_ids) == count:
            print(f"  [PASS] Successfully consumed and verified all {count} Avro events.")
            return True
        else:
            print(f"  [FAIL] Timed out: consumed {len(consumed_ids)}/{count} matching events.")
            return False
    except Exception as e:
        print(f"  [FAIL] Kafka error: {e}")
        return False


async def run_smoke_test() -> int:
    settings = get_settings()
    print("==================================================")
    print("        PulseStream Phase 1 Smoke Test           ")
    print("==================================================")
    print(f"Kafka Bootstrap Servers: {settings.kafka_bootstrap_servers}")
    print(f"Redis URL:               {settings.redis_url}")
    print(f"Postgres DSN:            {settings.postgres_dsn}")
    print("==================================================")

    redis_ok = await verify_redis(settings.redis_url)
    postgres_ok = await verify_postgres(settings.postgres_dsn)
    kafka_ok = verify_kafka(settings.kafka_bootstrap_servers, settings.kafka_events_topic, count=100)

    print("\n----------------- SMOKE TEST SUMMARY -----------------")
    print(f"Redis:      {'PASS' if redis_ok else 'FAIL / NOT REACHABLE'}")
    print(f"PostgreSQL: {'PASS' if postgres_ok else 'FAIL / NOT REACHABLE'}")
    print(f"Kafka:      {'PASS' if kafka_ok else 'FAIL / NOT REACHABLE'}")
    print("------------------------------------------------------")

    all_passed = redis_ok and postgres_ok and kafka_ok
    if all_passed:
        print("ALL PHASE 1 SMOKE TESTS PASSED!")
        return 0
    else:
        print("Note: When containers are started via 'make up', rerun 'make smoke'.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(run_smoke_test())
    sys.exit(exit_code)
