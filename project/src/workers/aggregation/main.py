"""Entry point for the Aggregation Worker service."""

import asyncio
import signal

from src.common.config import get_settings
from src.common.logging import get_logger, setup_logging
from src.workers.aggregation.aggregator import AggregationWorker

logger = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    setup_logging(log_level=settings.log_level)
    logger.info("Starting PulseStream Aggregation Worker...")

    worker = AggregationWorker(
        group_id=settings.kafka_consumer_group,
        health_port=settings.worker_health_port,
        settings=settings,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal() -> None:
        logger.info("Received termination signal, stopping worker...")
        loop.create_task(worker.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # On Windows add_signal_handler is not fully implemented for all loops
            signal.signal(sig, lambda s, f: _handle_signal())

    try:
        loop.run_until_complete(worker.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        loop.run_until_complete(worker.shutdown())
        loop.close()
        logger.info("Aggregation Worker shutdown complete.")


if __name__ == "__main__":
    main()
