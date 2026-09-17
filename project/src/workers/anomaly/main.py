"""Entry point for the Anomaly Detection Worker service."""

import asyncio
import signal

from src.common.config import get_settings
from src.common.logging import get_logger, setup_logging
from src.workers.anomaly.detector import AnomalyDetectionWorker

logger = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    setup_logging(log_level=settings.log_level)
    logger.info("Starting PulseStream Anomaly Detection Worker...")

    worker = AnomalyDetectionWorker(
        group_id="ps-anomaly",
        health_port=9102,
        settings=settings,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal() -> None:
        logger.info("Received termination signal, stopping anomaly worker...")
        loop.create_task(worker.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: _handle_signal())

    try:
        loop.run_until_complete(worker.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        loop.run_until_complete(worker.shutdown())
        loop.close()
        logger.info("Anomaly Detection Worker shutdown complete.")


if __name__ == "__main__":
    main()
