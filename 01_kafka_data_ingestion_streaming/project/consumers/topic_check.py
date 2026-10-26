"""Checks whether topics exist on the Kafka cluster"""
import os

from confluent_kafka.admin import AdminClient


# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}


def topic_exists(topic: str) -> bool:
    """
    Check whether a topic exists on the cluster.

    Parameters
    ----------
    topic : str
        Name of the topic to look for.

    Returns
    -------
    bool
        ``True`` if the topic exists, ``False`` otherwise.
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)
    topic_metadata = client.list_topics(timeout=5)
    topics = topic_metadata.topics
    return topic in topics
