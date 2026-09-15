"""PulseStream Ingestion Service.

Contains connectors (Wikipedia SSE and synthetic event simulator),
the validation/enrichment ingestion pipeline, and service entrypoint.
"""

from src.ingestion.pipeline import IngestionPipeline

__all__ = ["IngestionPipeline"]
