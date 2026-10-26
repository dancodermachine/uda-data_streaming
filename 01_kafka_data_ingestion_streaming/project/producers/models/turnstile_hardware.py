"""Simulates turnstile hardware that counts riders entering a station"""
from datetime import datetime, timedelta
import logging
import math
from pathlib import Path
import random
from typing import TYPE_CHECKING

import pandas as pd

from models.producer import Producer

# station.py imports this module (via turnstile.py), so import Station for type hints only
if TYPE_CHECKING:
    from models.station import Station


logger = logging.getLogger(__name__)


class TurnstileHardware:
    """Generates simulated rider entry counts from historical ridership data"""

    curve_df: pd.DataFrame | None = None
    seed_df: pd.DataFrame | None = None

    def __init__(self, station: "Station") -> None:
        """
        Initialize the turnstile hardware for a station.

        Loads the ridership data on first use and reads the station's average
        weekday, Saturday and Sunday/holiday ridership.

        Parameters
        ----------
        station : Station
            Station the hardware is installed at.

        Returns
        -------
        None
        """
        self.station = station
        TurnstileHardware._load_data()
        self.metrics_df = TurnstileHardware.seed_df[
            TurnstileHardware.seed_df["station_id"] == station.station_id
        ]
        self.weekday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_weekday_rides"])
        )
        self.saturday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_saturday_rides"])
        )
        self.sunday_ridership = int(
            round(self.metrics_df.iloc[0]["avg_sunday-holiday_rides"])
        )

    @classmethod
    def _load_data(cls) -> None:
        """
        Load the ridership CSVs once and cache them on the class.

        Reads ``ridership_curve.csv`` (share of daily riders per hour) and
        ``ridership_seed.csv`` (average daily riders per station) from the
        producers ``data`` directory.

        Returns
        -------
        None
        """
        if cls.curve_df is None:
            cls.curve_df = pd.read_csv(
                f"{Path(__file__).parents[1]}/data/ridership_curve.csv"
            )
        if cls.seed_df is None:
            cls.seed_df = pd.read_csv(
                f"{Path(__file__).parents[1]}/data/ridership_seed.csv"
            )

    def get_entries(self, timestamp: datetime, time_step: timedelta) -> int:
        """
        Estimate the number of turnstile entries for a simulation step.

        Scales the station's daily ridership by the share of riders expected
        in the current hour, divides it across the step, and adds a little
        random noise.

        Parameters
        ----------
        timestamp : datetime
            Simulated time of this step; its hour and weekday pick the
            ridership figures.
        time_step : timedelta
            Simulated time covered by one step.

        Returns
        -------
        int
            Number of riders entering during this step, never negative.
        """
        hour_curve = TurnstileHardware.curve_df[
            TurnstileHardware.curve_df["hour"] == timestamp.hour
        ]
        ratio = hour_curve.iloc[0]["ridership_ratio"]
        total_steps = int(60 / (60 / time_step.total_seconds()))

        num_riders = 0
        dow = timestamp.weekday()
        if dow >= 0 or dow < 5:
            num_riders = self.weekday_ridership
        elif dow == 6:
            num_riders = self.saturday_ridership
        else:
            num_riders = self.sunday_ridership

        # Calculate approximation of number of entries for this simulation step
        num_entries = int(math.floor(num_riders * ratio / total_steps))
        # Introduce some randomness in the data
        return max(num_entries + random.choice(range(-5, 5)), 0)
