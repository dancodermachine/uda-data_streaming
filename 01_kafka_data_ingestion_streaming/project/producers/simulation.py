"""Defines a time simulation responsible for executing any registered
producers
"""
import datetime
import time
from enum import IntEnum
import logging
import logging.config
from pathlib import Path

import pandas as pd

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

from connector import configure_connector
from models import Line, Weather


logger = logging.getLogger(__name__)


class TimeSimulation:
    """Drives the CTA simulation clock and runs every producer on each tick"""

    weekdays = IntEnum("weekdays", "mon tue wed thu fri sat sun", start=0)
    ten_min_frequency = datetime.timedelta(minutes=10)

    def __init__(
        self,
        sleep_seconds: int = 5,
        time_step: datetime.timedelta | None = None,
        schedule: dict[IntEnum, dict[int, datetime.timedelta]] | None = None,
    ) -> None:
        """
        Initialize the time simulation and build the train lines.

        Loads the stations CSV and creates the blue, red and green lines,
        which in turn create every station, turnstile and train producer.

        Parameters
        ----------
        sleep_seconds : int, optional
            Real seconds to wait between simulation steps. Defaults to ``5``.
        time_step : datetime.timedelta | None, optional
            Simulated time covered by one step. Defaults to ``None``, which
            uses ``sleep_seconds`` minutes.
        schedule : dict[IntEnum, dict[int, datetime.timedelta]] | None, optional
            Train frequency per weekday (a ``TimeSimulation.weekdays`` member),
            keyed by hour of day. Defaults to ``None``, which runs trains every
            ten minutes all week.

        Returns
        -------
        None
        """
        self.sleep_seconds = sleep_seconds
        self.time_step = time_step
        if self.time_step is None:
            self.time_step = datetime.timedelta(minutes=self.sleep_seconds)

        # Read data from disk
        self.raw_df = pd.read_csv(
            f"{Path(__file__).parents[0]}/data/cta_stations.csv"
        ).sort_values("order")

        # Define the train schedule (same for all trains)
        self.schedule = schedule
        if schedule is None:
            self.schedule = {
                TimeSimulation.weekdays.mon: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.tue: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.wed: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.thu: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.fri: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.sat: {0: TimeSimulation.ten_min_frequency},
                TimeSimulation.weekdays.sun: {0: TimeSimulation.ten_min_frequency},
            }

        self.train_lines = [
            Line(Line.colors.blue, self.raw_df[self.raw_df["blue"]]),
            Line(Line.colors.red, self.raw_df[self.raw_df["red"]]),
            Line(Line.colors.green, self.raw_df[self.raw_df["green"]]),
        ]

    def run(self) -> None:
        """
        Run the simulation until interrupted with Ctrl+C.

        Registers the Kafka Connect stations connector, then on every step
        sends a weather event at the top of each simulated hour, advances
        turnstiles and trains on every line, and sleeps ``sleep_seconds``.
        On Ctrl+C, closes every line so queued messages are flushed.

        Returns
        -------
        None
        """
        curr_time = datetime.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        logger.info("Beginning simulation, press Ctrl+C to exit at any time")
        logger.info("loading kafka connect jdbc source connector")
        configure_connector()

        logger.info("beginning cta train simulation")
        weather = Weather(curr_time.month)
        try:
            while True:
                logger.debug("simulation running: %s", curr_time.isoformat())
                # Send weather on the top of the hour
                if curr_time.minute == 0:
                    weather.run(curr_time.month)
                _ = [line.run(curr_time, self.time_step) for line in self.train_lines]
                curr_time = curr_time + self.time_step
                time.sleep(self.sleep_seconds)
        except KeyboardInterrupt as e:
            logger.info("Shutting down")
            _ = [line.close() for line in self.train_lines]


if __name__ == "__main__":
    TimeSimulation().run()
