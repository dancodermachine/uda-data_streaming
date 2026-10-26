"""Producer base-class providing common utilites and functionality"""
import logging
import os
import time


from avro.schema import Schema
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics: set[str] = set([])

    def __init__(
        self,
        topic_name: str,
        key_schema: Schema,
        value_schema: Schema | None = None,
        num_partitions: int = 1,
        num_replicas: int = 3,
        topic_config: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize a Producer connected to Confluent Cloud.

        Builds the Kafka and Schema Registry connection settings from the
        ``CONFLUENT_*`` environment variables, creates the topic if this
        process has not already done so, and configures an ``AvroProducer``
        that serializes keys and values with the given schemas.

        Parameters
        ----------
        topic_name : str
            Name of the Kafka topic to produce messages into.
        key_schema : Schema
            Avro schema used to serialize message keys.
        value_schema : Schema | None, optional
            Avro schema used to serialize message values. Defaults to ``None``.
        num_partitions : int, optional
            Number of partitions to create the topic with. Defaults to ``1``.
        num_replicas : int, optional
            Replication factor to create the topic with. Defaults to ``3``,
            the value Confluent Cloud requires.
        topic_config : dict[str, str] | None, optional
            Topic-level settings to create the topic with, e.g.
            ``{"retention.ms": "604800000"}``. Defaults to ``None``, which
            uses the cluster defaults.

        Returns
        -------
        None
        """
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.topic_config = topic_config or {}

        # Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`, and its
        # Schema Registry uses its own API key, separate from the Kafka one.
        self.broker_properties: dict[str, str] = {
            "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
            "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
            "schema.registry.url": os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"],
            "schema.registry.basic.auth.credentials.source": "USER_INFO",
            "schema.registry.basic.auth.user.info": (
                f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_KEY"]}:'
                f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_SECRET"]}'
            ),
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self) -> None:
        """
        Create the producer topic if it does not already exist.

        Checks the cluster metadata for ``topic_name`` and, if it is missing,
        creates it with ``num_partitions`` partitions, a replication factor
        of ``num_replicas`` and the settings in ``topic_config``.

        Returns
        -------
        None

        Raises
        ------
        KafkaException
            If the broker rejects the topic creation request.
        """
        # AdminClient rejects the schema.registry.* keys, so pass only the Kafka settings
        client = AdminClient(
            {k: v for k, v in self.broker_properties.items() if not k.startswith("schema.registry.")}
        )
        if self.topic_name in client.list_topics(timeout=5).topics:
            logger.info("topic %s already exists - skipping creation", self.topic_name)
            return

        futures = client.create_topics(
            [
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config=self.topic_config,
                )
            ]
        )
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic %s created", topic)
            except Exception as e:
                logger.error("failed to create topic %s: %s", topic, e)
                raise

    def close(self) -> None:
        """
        Prepare the producer for exit.

        Flushes the producer so every queued message is delivered before the
        process shuts down.

        Returns
        -------
        None
        """
        self.producer.flush()

    def time_millis(self) -> int:
        """
        Get the current time in milliseconds, used as the key for Kafka events.

        Returns
        -------
        int
            Milliseconds since the Unix epoch, rounded to the nearest integer.
        """
        return int(round(time.time() * 1000))
