"""Service entrypoint for the PulseStream Ingestion Service."""

import asyncio
import signal
import sys
from typing import List

from src.common.config import get_settings
from src.common.logging import get_logger, setup_logging
from src.common.metrics import start_metrics_exporter
from src.ingestion.connectors.devto import DevtoConnector
from src.ingestion.connectors.github_events import GitHubEventsConnector
from src.ingestion.connectors.hackernews import HackerNewsConnector
from src.ingestion.connectors.reddit import RedditConnector
from src.ingestion.connectors.simulator import EventSimulator
from src.ingestion.connectors.wikipedia import WikipediaConnector
from src.ingestion.pipeline import IngestionPipeline

logger = get_logger("pulsestream.ingestion")


async def run_simulator(pipeline: IngestionPipeline, simulator: EventSimulator) -> None:
    """Consumes events from EventSimulator and feeds them into the IngestionPipeline."""
    logger.info(
        "Starting Simulator task (mode=%s, rate=%d)...",
        simulator.mode,
        simulator.target_rate,
    )
    try:
        async for event in simulator.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("Simulator task cancelled.")
    finally:
        simulator.stop()


async def run_wikipedia(pipeline: IngestionPipeline, wikipedia: WikipediaConnector) -> None:
    """Consumes events from Wikipedia SSE stream and feeds them into the IngestionPipeline."""
    logger.info("Starting Wikipedia SSE connector task...")
    try:
        async for event in wikipedia.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("Wikipedia connector task cancelled.")
    finally:
        wikipedia.stop()


async def run_hackernews(pipeline: IngestionPipeline, hackernews: HackerNewsConnector) -> None:
    """Consumes events from Hacker News API and feeds them into the IngestionPipeline."""
    logger.info("Starting Hacker News connector task...")
    try:
        async for event in hackernews.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("Hacker News connector task cancelled.")
    finally:
        hackernews.stop()


async def run_github(pipeline: IngestionPipeline, github: GitHubEventsConnector) -> None:
    """Consumes events from GitHub Events API and feeds them into the IngestionPipeline."""
    logger.info("Starting GitHub Events connector task...")
    try:
        async for event in github.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("GitHub connector task cancelled.")
    finally:
        github.stop()


async def run_reddit(pipeline: IngestionPipeline, reddit: RedditConnector) -> None:
    """Consumes events from Reddit JSON API and feeds them into the IngestionPipeline."""
    logger.info("Starting Reddit connector task...")
    try:
        async for event in reddit.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("Reddit connector task cancelled.")
    finally:
        reddit.stop()


async def run_devto(pipeline: IngestionPipeline, devto: DevtoConnector) -> None:
    """Consumes articles from Dev.to API and feeds them into the IngestionPipeline."""
    logger.info("Starting Dev.to connector task...")
    try:
        async for event in devto.stream_events():
            pipeline.process_event(event)
    except asyncio.CancelledError:
        logger.info("Dev.to connector task cancelled.")
    finally:
        devto.stop()


async def main() -> None:
    settings = get_settings()
    setup_logging(log_level=settings.log_level)

    logger.info("Initializing PulseStream Ingestion Service...")
    logger.info("Kafka Bootstrap Servers: %s", settings.kafka_bootstrap_servers)
    logger.info("Events Topic: %s", settings.kafka_events_topic)
    logger.info("DLQ Topic: %s", settings.kafka_dlq_topic)

    # Start Prometheus metrics exporter
    start_metrics_exporter(port=settings.metrics_port)

    pipeline = IngestionPipeline()
    tasks: List[asyncio.Task] = []
    connectors = []

    # 1. Simulator connector
    if settings.ingestion_simulator_enabled:
        simulator = EventSimulator(
            mode=settings.ingestion_simulator_mode,
            rate=settings.ingestion_simulator_rate,
            chaos_duplicate_pct=settings.ingestion_chaos_duplicate_pct,
            chaos_malformed_pct=settings.ingestion_chaos_malformed_pct,
        )
        connectors.append(simulator)
        sim_task = asyncio.create_task(
            run_simulator(pipeline, simulator),
            name="simulator_worker",
        )
        tasks.append(sim_task)

    # 2. Wikipedia SSE connector
    if settings.ingestion_wikipedia_enabled:
        wikipedia = WikipediaConnector(
            stream_url=settings.ingestion_wikipedia_url,
        )
        connectors.append(wikipedia)
        wiki_task = asyncio.create_task(
            run_wikipedia(pipeline, wikipedia),
            name="wikipedia_worker",
        )
        tasks.append(wiki_task)

    # 3. Hacker News connector
    if settings.ingestion_hackernews_enabled:
        hackernews = HackerNewsConnector(
            poll_interval=float(settings.ingestion_hackernews_poll_interval),
        )
        connectors.append(hackernews)
        hn_task = asyncio.create_task(
            run_hackernews(pipeline, hackernews),
            name="hackernews_worker",
        )
        tasks.append(hn_task)

    # 4. GitHub Events connector
    if settings.ingestion_github_enabled:
        github = GitHubEventsConnector(
            poll_interval=float(settings.ingestion_github_poll_interval),
        )
        connectors.append(github)
        gh_task = asyncio.create_task(
            run_github(pipeline, github),
            name="github_worker",
        )
        tasks.append(gh_task)

    # 5. Reddit Tech Feed connector
    if settings.ingestion_reddit_enabled:
        reddit = RedditConnector(
            poll_interval=float(settings.ingestion_reddit_poll_interval),
        )
        connectors.append(reddit)
        reddit_task = asyncio.create_task(
            run_reddit(pipeline, reddit),
            name="reddit_worker",
        )
        tasks.append(reddit_task)

    # 6. Dev.to Community connector
    if settings.ingestion_devto_enabled:
        devto = DevtoConnector(
            poll_interval=float(settings.ingestion_devto_poll_interval),
            per_page=settings.ingestion_devto_per_page,
        )
        connectors.append(devto)
        devto_task = asyncio.create_task(
            run_devto(pipeline, devto),
            name="devto_worker",
        )
        tasks.append(devto_task)

    if not tasks:
        logger.warning("No ingestion connectors enabled! Exiting.")
        return

    # Graceful shutdown event
    stop_event = asyncio.Event()

    def handle_shutdown_signal(*_args):
        logger.info("Shutdown signal received, initiating graceful drain...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    # Register signal handlers if supported (Windows has SIGINT/SIGTERM via signal module)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown_signal)
        except NotImplementedError:
            # Fallback on Windows
            signal.signal(sig, handle_shutdown_signal)

    logger.info("PulseStream Ingestion Service running with %d connector(s).", len(tasks))

    # Wait for shutdown signal or task termination
    done, pending = await asyncio.wait(
        [asyncio.create_task(stop_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    logger.info("Stopping connectors and draining in-flight events...")
    for connector in connectors:
        connector.stop()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    # Flush Kafka producer buffer
    logger.info("Flushing Kafka producer...")
    remaining = pipeline.flush(timeout=10.0)
    logger.info(
        "Producer flushed (remaining un-delivered: %d). Ingestion shutdown complete.", remaining
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Terminated by user.")
        sys.exit(0)
