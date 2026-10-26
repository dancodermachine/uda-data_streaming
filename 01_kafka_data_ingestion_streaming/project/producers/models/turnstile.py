"""Creates a turnstile data producer"""
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

# station.py imports this module, so import Station for type hints only
if TYPE_CHECKING:
    from models.station import Station


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    """Produces rider entry events for a single station's turnstile"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station: "Station") -> None:
        """
        Initialize the turnstile for a station.

        Connects to the ``org.chicago.cta.turnstile`` topic shared by every
        station (creating it on first use) through the parent ``Producer``,
        and attaches the simulated turnstile hardware that generates rider
        counts.

        Parameters
        ----------
        station : Station
            Station the turnstile belongs to.

        Returns
        -------
        None
        """
        # One topic for every station, so KSQL can count entries per station in a single query
        super().__init__(
            "org.chicago.cta.turnstile",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            # Confluent Cloud requires a replication factor of 3
            num_replicas=3,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp: datetime, time_step: timedelta) -> None:
        """
        Simulate riders entering through the turnstile.

        Asks the turnstile hardware how many riders entered during this
        simulation step and produces one turnstile event per rider, so
        KSQL can count entries per station.

        Parameters
        ----------
        timestamp : datetime
            Simulated time of this step.
        time_step : timedelta
            Simulated time covered by one step.

        Returns
        -------
        None
        """
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,
                },
            )
