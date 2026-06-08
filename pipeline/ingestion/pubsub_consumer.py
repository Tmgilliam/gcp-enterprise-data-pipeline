"""
Pub/Sub Event Consumer — Real-time ERP event ingestion.

Consumes messages from erp-transactions, inventory-events, and order-events
topics. Routes to BigQuery streaming insert or Dataflow for heavy transforms.

Multi-cloud equivalent:
  - Azure Event Hubs consumer
  - AWS Kinesis consumer
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

TOPIC_CONFIG = {
    "erp-transactions": {
        "subscription": "erp-transactions-dataflow",
        "event_types": ["po_created", "po_received", "invoice_posted"],
        "bq_table": "raw.erp_transactions_events",
    },
    "inventory-events": {
        "subscription": "inventory-events-dataflow",
        "event_types": ["pick", "putaway", "cycle_count", "adjustment"],
        "bq_table": "raw.inventory_events",
    },
    "order-events": {
        "subscription": "order-events-dataflow",
        "event_types": ["order_placed", "order_shipped", "order_delayed", "order_cancelled"],
        "bq_table": "raw.order_events",
    },
}


@dataclass
class PubSubMessage:
    """Parsed Pub/Sub message with metadata."""

    message_id: str
    topic: str
    event_type: str
    payload: dict[str, Any]
    publish_time: datetime
    attributes: dict[str, str]
    delivery_attempt: int = 0


class PubSubConsumer:
    """
    Real-time event consumer for ERP Pub/Sub topics.

    Supports pull-based consumption with ack/nack, dead letter routing,
    and message validation before downstream processing.
    """

    def __init__(
        self,
        project_id: str,
        subscription_id: str,
        max_messages: int = 100,
        ack_deadline_seconds: int = 60,
    ):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.max_messages = max_messages
        self.ack_deadline_seconds = ack_deadline_seconds
        self._subscriber = pubsub_v1.SubscriberClient()
        self._subscription_path = self._subscriber.subscription_path(
            project_id, subscription_id
        )

    def parse_message(self, message: pubsub_v1.types.PubsubMessage, topic: str) -> PubSubMessage:
        """Parse raw Pub/Sub message into structured format."""
        try:
            payload = json.loads(message.data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw_data": message.data.decode("utf-8", errors="replace")}

        return PubSubMessage(
            message_id=message.message_id,
            topic=topic,
            event_type=message.attributes.get("event_type", payload.get("event_type", "unknown")),
            payload=payload,
            publish_time=message.publish_time,
            attributes=dict(message.attributes),
            delivery_attempt=int(message.attributes.get("googclient_deliveryattempt", "0")),
        )

    def validate_message(self, message: PubSubMessage) -> tuple[bool, list[str]]:
        """Validate message structure before processing."""
        errors = []

        if message.event_type == "unknown":
            errors.append("Missing event_type in attributes or payload")

        if not message.payload:
            errors.append("Empty payload")

        config = TOPIC_CONFIG.get(message.topic, {})
        if config and message.event_type not in config.get("event_types", []):
            if message.event_type != "unknown":
                logger.warning(
                    "Unexpected event_type '%s' for topic '%s'",
                    message.event_type,
                    message.topic,
                )

        return len(errors) == 0, errors

    def pull_messages(self, max_messages: Optional[int] = None) -> list[tuple[PubSubMessage, str]]:
        """
        Pull messages from subscription.

        Returns:
            List of (parsed_message, ack_id) tuples.
        """
        limit = max_messages or self.max_messages

        response = self._subscriber.pull(
            request={
                "subscription": self._subscription_path,
                "max_messages": limit,
            },
            timeout=30,
        )

        results = []
        topic = self.subscription_id.replace("-dataflow", "").replace(f"-{self.project_id}", "")

        for received in response.received_messages:
            parsed = self.parse_message(received.message, topic)
            results.append((parsed, received.ack_id))

        logger.info("Pulled %d messages from %s", len(results), self.subscription_id)
        return results

    def ack_message(self, ack_id: str) -> None:
        """Acknowledge successful message processing."""
        self._subscriber.acknowledge(
            request={
                "subscription": self._subscription_path,
                "ack_ids": [ack_id],
            }
        )

    def nack_message(self, ack_id: str) -> None:
        """Negative acknowledge — message will be redelivered or routed to DLQ."""
        self._subscriber.modify_ack_deadline(
            request={
                "subscription": self._subscription_path,
                "ack_ids": [ack_id],
                "ack_deadline_seconds": 0,
            }
        )

    def consume_batch(
        self,
        handler: Callable[[PubSubMessage], bool],
        max_batches: int = 10,
    ) -> dict[str, int]:
        """
        Pull, validate, process, and ack/nack messages in batches.

        Args:
            handler: Function that processes a message and returns True on success.
            max_batches: Maximum number of pull batches to process.

        Returns:
            Summary dict with processed, failed, and invalid counts.
        """
        stats = {"processed": 0, "failed": 0, "invalid": 0, "batches": 0}

        for _ in range(max_batches):
            messages = self.pull_messages()
            if not messages:
                break

            stats["batches"] += 1

            for parsed, ack_id in messages:
                valid, errors = self.validate_message(parsed)
                if not valid:
                    stats["invalid"] += 1
                    logger.warning(
                        "Invalid message %s: %s",
                        parsed.message_id,
                        errors,
                    )
                    self.nack_message(ack_id)
                    continue

                try:
                    success = handler(parsed)
                    if success:
                        self.ack_message(ack_id)
                        stats["processed"] += 1
                    else:
                        self.nack_message(ack_id)
                        stats["failed"] += 1
                except Exception as exc:
                    logger.error(
                        "Handler failed for message %s: %s",
                        parsed.message_id,
                        exc,
                    )
                    self.nack_message(ack_id)
                    stats["failed"] += 1

        logger.info("Batch consumption complete", extra=stats)
        return stats
