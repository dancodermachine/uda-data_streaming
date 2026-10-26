"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

import requests
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

TOPIC_PREFIX = "org.chicago.cta."
TABLE_NAME = "stations"
# The connector writes each table to `<topic.prefix><table>`
TOPIC_NAME = f"{TOPIC_PREFIX}{TABLE_NAME}"

# Kafka Connect runs in Docker, so it reaches Postgres by its docker-compose service name
POSTGRES_URL = "jdbc:postgresql://postgres:5432/cta"
POSTGRES_USER = "cta_admin"
POSTGRES_PASSWORD = "chicago"

# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}


def create_topic() -> None:
    """
    Create the connector's output topic if it does not already exist.

    Confluent Cloud does not auto-create topics, and this Kafka Connect
    version cannot create them for a connector, so the topic must exist
    before the connector starts writing.

    Returns
    -------
    None

    Raises
    ------
    KafkaException
        If the broker rejects the topic creation request.
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)
    if TOPIC_NAME in client.list_topics(timeout=5).topics:
        logger.debug("topic %s already exists - skipping creation", TOPIC_NAME)
        return

    futures = client.create_topics(
        [
            NewTopic(
                TOPIC_NAME,
                num_partitions=1,
                # Confluent Cloud requires a replication factor of 3
                replication_factor=3,
                config={
                    # Stations are reference data that Kafka Connect's incrementing mode loads
                    # only once, so they must never expire. Compaction is not an option because
                    # the connector writes null keys.
                    "cleanup.policy": "delete",
                    "retention.ms": "-1",
                },
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


def configure_connector() -> None:
    """
    Start and configure the Kafka Connect JDBC source connector.

    Registers a connector that loads the Postgres ``stations`` table into
    the ``org.chicago.cta.stations`` topic, using ``stop_id`` as the
    incrementing column. Does nothing if the connector already exists.

    Returns
    -------
    None

    Raises
    ------
    requests.HTTPError
        If Kafka Connect rejects the connector configuration.
    """
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    create_topic()

    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Station data rarely changes, so poll infrequently (once a day).
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": POSTGRES_URL,
                "connection.user": POSTGRES_USER,
                "connection.password": POSTGRES_PASSWORD,
                "table.whitelist": TABLE_NAME,
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": TOPIC_PREFIX,
                "poll.interval.ms": "86400000",
            }
        }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
