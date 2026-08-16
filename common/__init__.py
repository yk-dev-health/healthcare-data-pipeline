"""Shared building blocks for the healthcare ingestion pipeline.

This package holds the pieces that both the API (producer) and the worker
(consumer) depend on, so that a single definition of "what a valid clinical
event is" and "which failures are retryable" is enforced on both sides of the
Pub/Sub boundary.
"""
