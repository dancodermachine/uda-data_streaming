"""Methods pertaining to weather data"""
import base64
from enum import IntEnum
import json
import logging
import os
from pathlib import Path
import random

import requests
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    # Confluent Cloud serves its Kafka REST v3 API on the same host as the bootstrap server
    rest_url = f'https://{os.environ["CONFLUENT_BOOTSTRAP_SERVERS"].split(":")[0]}'
    # Looked up once from the cluster on first use, see `_records_url`
    cluster_id: str | None = None

    key_serializer: AvroSerializer | None = None
    value_serializer: AvroSerializer | None = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month: int) -> None:
        """
        Initialize the weather model and its topic.

        Creates the ``org.chicago.cta.weather.v1`` topic through the parent
        ``Producer``, sets a starting temperature for the season, and builds
        the Avro key and value serializers from the weather schemas.

        Parameters
        ----------
        month : int
            Month of the year as given by ``datetime.month`` (``1`` is January), used to pick the
            starting temperature.

        Returns
        -------
        None
        """
        # Events go through the REST API rather than the parent's AvroProducer, so the
        # schemas live in the serializers below instead of being passed up here
        super().__init__(
            "org.chicago.cta.weather.v1",
            key_schema=None,
            num_partitions=1,
            # Confluent Cloud requires a replication factor of 3
            num_replicas=3,
            topic_config={
                # Weather is an event history keyed by unique timestamps, so compaction would
                # remove nothing; keep a week of events instead
                "cleanup.policy": "delete",
                "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            },
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_serializer is None or Weather.value_serializer is None:
            # Confluent Cloud Schema Registry uses its own API key, separate from the Kafka one
            schema_registry = SchemaRegistryClient({
                "url": os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"],
                "basic.auth.user.info": (
                    f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_KEY"]}:'
                    f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_SECRET"]}'
                ),
            })
            schemas = Path(__file__).parents[0] / "schemas"
            Weather.key_serializer = AvroSerializer(
                schema_registry, (schemas / "weather_key.json").read_text()
            )
            Weather.value_serializer = AvroSerializer(
                schema_registry, (schemas / "weather_value.json").read_text()
            )

    def _set_weather(self, month: int) -> None:
        """
        Advance the simulated temperature and pick a new weather status.

        Nudges the temperature with a triangular random step that leans colder
        in winter and warmer in summer, then picks a random status.

        Parameters
        ----------
        month : int
            Month of the year as given by ``datetime.month`` (``1`` is January).

        Returns
        -------
        None
        """
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def _records_url(self) -> str:
        """
        Build the Kafka REST v3 URL for producing to the weather topic.

        Fetches the cluster ID from the cluster on first call and caches it
        on the class, so no extra environment variable is needed.

        Returns
        -------
        str
            ``<rest_url>/kafka/v3/clusters/<cluster_id>/topics/<topic>/records``.
        """
        if Weather.cluster_id is None:
            # AdminClient rejects the schema.registry.* keys, so pass only the Kafka settings
            client = AdminClient(
                {k: v for k, v in self.broker_properties.items() if not k.startswith("schema.registry.")}
            )
            Weather.cluster_id = client.describe_cluster(request_timeout=10).result().cluster_id
        return f"{Weather.rest_url}/kafka/v3/clusters/{Weather.cluster_id}/topics/{self.topic_name}/records"

    def _encode(self, serializer: AvroSerializer, field: str, data: dict) -> dict[str, str]:
        """
        Avro-encode a key or value for the Kafka REST v3 API.

        Confluent Cloud's REST API cannot apply Schema Registry schemas itself,
        so the record is serialized here (registering the schema on first use)
        and sent as base64-encoded ``BINARY`` data.

        Parameters
        ----------
        serializer : AvroSerializer
            Serializer holding the key or value schema.
        field : str
            ``MessageField.KEY`` or ``MessageField.VALUE``, used to pick the
            Schema Registry subject (``<topic>-key`` or ``<topic>-value``).
        data : dict
            Record to encode; must match the serializer's schema.

        Returns
        -------
        dict[str, str]
            ``{"type": "BINARY", "data": <base64 Avro bytes>}``.
        """
        encoded = serializer(data, SerializationContext(self.topic_name, field))
        return {"type": "BINARY", "data": base64.b64encode(encoded).decode("ascii")}

    def run(self, month: int) -> None:
        """
        Update the weather and produce a weather event through the REST API.

        Avro-encodes the key and value with the weather schemas and POSTs the
        record to Confluent Cloud's Kafka REST v3 API.

        Parameters
        ----------
        month : int
            Month of the year as given by ``datetime.month`` (``1`` is January).

        Returns
        -------
        None

        Raises
        ------
        requests.HTTPError
            If the REST API rejects the request.
        """
        self._set_weather(month)

        # See: https://docs.confluent.io/cloud/current/kafka-rest/krest-qs.html
        resp = requests.post(
            self._records_url(),
            auth=(os.environ["CONFLUENT_KAFKA_KEY"], os.environ["CONFLUENT_KAFKA_SECRET"]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "key": self._encode(
                        Weather.key_serializer, MessageField.KEY, {"timestamp": self.time_millis()}
                    ),
                    "value": self._encode(
                        Weather.value_serializer,
                        MessageField.VALUE,
                        {"temperature": self.temp, "status": self.status.name},
                    ),
                }
            ),
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            logger.error("failed to send weather data to the REST API: %s", resp.text)
            raise

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
