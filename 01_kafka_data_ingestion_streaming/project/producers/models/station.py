"""Methods pertaining to loading and configuring CTA "L" station data."""
from enum import IntEnum
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer
from models.train import Train


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(
        self,
        station_id: int,
        name: str,
        color: IntEnum,
        direction_a: "Station | None" = None,
        direction_b: "Station | None" = None,
    ) -> None:
        """
        Initialize a Station and its arrivals topic.

        Derives a Kafka-safe topic name from the station name, creates the
        ``org.chicago.cta.station.arrivals.<station_name>`` topic through the
        parent ``Producer``, and attaches a ``Turnstile`` producer for the
        station.

        Parameters
        ----------
        station_id : int
            CTA identifier of the station.
        name : str
            Human-readable station name, e.g. ``"Clark/Lake"``.
        color : IntEnum
            Line colour the station belongs to (a ``Line.colors`` member).
        direction_a : Station | None, optional
            Neighbouring station in the 'a' direction. Defaults to ``None``.
        direction_b : Station | None, optional
            Neighbouring station in the 'b' direction. Defaults to ``None``.

        Returns
        -------
        None
        """
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # One topic per station; the consumer server subscribes to `^org.chicago.cta.station.arrivals.`
        topic_name = f"org.chicago.cta.station.arrivals.{station_name}"
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            num_partitions=1,
            # Confluent Cloud requires a replication factor of 3
            num_replicas=3,
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train: Train | None = None
        self.b_train: Train | None = None
        self.turnstile = Turnstile(self)


    def run(
        self,
        train: Train,
        direction: str,
        prev_station_id: int | None,
        prev_direction: str | None,
    ) -> None:
        """
        Produce an arrival event for a train reaching this station.

        Sends one Avro-serialized message to the station's arrivals topic,
        keyed by the current timestamp in milliseconds.

        Parameters
        ----------
        train : Train
            Train arriving at the station.
        direction : str
            Direction of travel, either ``"a"`` or ``"b"``.
        prev_station_id : int | None
            Identifier of the station the train came from, or ``None`` when
            the train is first placed on the line.
        prev_direction : str | None
            Direction the train was travelling at the previous station, or
            ``None`` when the train is first placed on the line.

        Returns
        -------
        None
        """
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                "station_id": self.station_id,
                "train_id": train.train_id,
                "direction": direction,
                "line": self.color.name,
                "train_status": train.status.name,
                "prev_station_id": prev_station_id,
                "prev_direction": prev_direction,
            },
        )

    def __str__(self) -> str:
        """
        Format the station and the trains currently at it as a table row.

        Returns
        -------
        str
            Station id, name, and the train and next station in each
            direction, with ``"---"`` where there is none.
        """
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self) -> str:
        """
        Return the same table-row representation as ``__str__``.

        Returns
        -------
        str
            The string produced by ``__str__``.
        """
        return str(self)

    def arrive_a(self, train: Train, prev_station_id: int | None, prev_direction: str | None) -> None:
        """
        Record a train arriving at this station in the 'a' direction.

        Parameters
        ----------
        train : Train
            Train arriving at the station.
        prev_station_id : int | None
            Identifier of the station the train came from, or ``None`` when
            the train is first placed on the line.
        prev_direction : str | None
            Direction the train was travelling at the previous station, or
            ``None`` when the train is first placed on the line.

        Returns
        -------
        None
        """
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train: Train, prev_station_id: int | None, prev_direction: str | None) -> None:
        """
        Record a train arriving at this station in the 'b' direction.

        Parameters
        ----------
        train : Train
            Train arriving at the station.
        prev_station_id : int | None
            Identifier of the station the train came from, or ``None`` when
            the train is first placed on the line.
        prev_direction : str | None
            Direction the train was travelling at the previous station, or
            ``None`` when the train is first placed on the line.

        Returns
        -------
        None
        """
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self) -> None:
        """
        Prepare the station for exit.

        Closes the station's turnstile producer, then flushes the station's
        own arrival producer.

        Returns
        -------
        None
        """
        self.turnstile.close()
        super(Station, self).close()
