"""Defines core consumer functionality"""
from collections.abc import Callable
import logging
import os

import confluent_kafka
from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern: str,
        message_handler: Callable[[Message], None],
        is_avro: bool = True,
        offset_earliest: bool = False,
        sleep_secs: float = 1.0,
        consume_timeout: float = 0.1,
    ) -> None:
        """
        Create a consumer subscribed to a topic or topic pattern.

        Parameters
        ----------
        topic_name_pattern : str
            Topic name, or a regex starting with ``^`` to subscribe to every
            matching topic (e.g. ``"^org.chicago.cta.station.arrivals."``).
        message_handler : Callable[[Message], None]
            Function called with each message received.
        is_avro : bool, optional
            ``True`` to decode messages with Avro through Schema Registry,
            ``False`` to receive raw bytes (e.g. JSON). Defaults to ``True``.
        offset_earliest : bool, optional
            ``True`` to read every partition from the beginning on each start.
            Defaults to ``False``.
        sleep_secs : float, optional
            Seconds to wait after the topic has no more messages. Defaults to
            ``1.0``.
        consume_timeout : float, optional
            Seconds to wait for a message on each poll. Defaults to ``0.1``.

        Returns
        -------
        None
        """
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
        self.broker_properties: dict[str, str] = {
            "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
            "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
            # One group per subscription, so the consumers don't share partition assignments
            "group.id": f"org.chicago.cta.server.{topic_name_pattern.lstrip('^')}",
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        if is_avro is True:
            # Confluent Cloud Schema Registry uses its own API key, separate from the Kafka one
            self.broker_properties["schema.registry.url"] = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]
            self.broker_properties["schema.registry.basic.auth.credentials.source"] = "USER_INFO"
            self.broker_properties["schema.registry.basic.auth.user.info"] = (
                f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_KEY"]}:'
                f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_SECRET"]}'
            )
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # on_assign runs on every partition assignment, so it can rewind to the beginning
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Handle a new partition assignment.

        If ``offset_earliest`` is set, rewinds every assigned partition to its
        first message so the full history is read again.

        Parameters
        ----------
        consumer : Consumer
            Consumer the partitions were assigned to.
        partitions : list[TopicPartition]
            Partitions assigned to the consumer.

        Returns
        -------
        None
        """
        for partition in partitions:
            if self.offset_earliest is True:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self) -> None:
        """
        Consume messages forever without blocking the Tornado event loop.

        Drains every available message, then sleeps ``sleep_secs`` before
        polling again.

        Returns
        -------
        None
            Runs until the event loop stops.
        """
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self) -> int:
        """
        Poll for one message and pass it to the message handler.

        Returns
        -------
        int
            ``1`` if a message was processed, ``0`` if no message was
            available or it could not be read.
        """
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except SerializerError as e:
            logger.error("unable to deserialize message from %s: %s", self.topic_name_pattern, e)
            return 0

        if message is None:
            return 0
        if message.error() is not None:
            logger.error("error from consumer of %s: %s", self.topic_name_pattern, message.error())
            return 0

        self.message_handler(message)
        return 1

    def close(self) -> None:
        """
        Close the consumer, committing offsets and leaving its group.

        Returns
        -------
        None
        """
        self.consumer.close()
