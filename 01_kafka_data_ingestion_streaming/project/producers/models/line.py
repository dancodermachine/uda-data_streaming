"""Defines functionality relating to train lines"""
import collections
from datetime import datetime, timedelta
from enum import IntEnum
import logging

import pandas as pd

from models import Station, Train


logger = logging.getLogger(__name__)


class Line:
    """Contains Chicago Transit Authority (CTA) Elevated Loop Train ("L") Station Data"""

    colors = IntEnum("colors", "blue green red", start=0)
    num_directions = 2

    def __init__(self, color: IntEnum, station_data: pd.DataFrame, num_trains: int = 10) -> None:
        """
        Initialize a line, its stations and its trains.

        Parameters
        ----------
        color : IntEnum
            Line colour (a ``Line.colors`` member).
        station_data : pd.DataFrame
            Rows of the stations CSV for this line, in travel order, with at
            least ``station_name`` and ``station_id`` columns.
        num_trains : int, optional
            Number of trains to place on the line. Defaults to ``10``.

        Returns
        -------
        None
        """
        self.color = color
        self.num_trains = num_trains
        self.stations = self._build_line_data(station_data)
        # We must always discount the terminal station at the end of each direction
        self.num_stations = len(self.stations) - 1
        self.trains = self._build_trains()

    def _build_line_data(self, station_df: pd.DataFrame) -> list[Station]:
        """
        Construct all stations on the line and link them together.

        Each station is created with the previous one as its 'a' neighbour,
        and the previous station gets the new one as its 'b' neighbour.

        Parameters
        ----------
        station_df : pd.DataFrame
            Rows of the stations CSV for this line, in travel order.

        Returns
        -------
        list[Station]
            Stations in order from the 'a' end to the 'b' end of the line.
        """
        stations = station_df["station_name"].unique()

        station_data = station_df[station_df["station_name"] == stations[0]]
        line = [
            Station(station_data["station_id"].unique()[0], stations[0], self.color)
        ]
        prev_station = line[0]
        for station in stations[1:]:
            station_data = station_df[station_df["station_name"] == station]
            new_station = Station(
                station_data["station_id"].unique()[0],
                station,
                self.color,
                prev_station,
            )
            prev_station.dir_b = new_station
            prev_station = new_station
            line.append(new_station)
        return line

    def _build_trains(self) -> list[Train]:
        """
        Construct the line's trains and spread them across its stations.

        Trains are named ``<colour initial>L<3-digit number>`` (e.g.
        ``"BL001"``) and placed at evenly spaced stations, producing an
        arrival event for each.

        Returns
        -------
        list[Train]
            The trains created, all in service.
        """
        trains = []
        curr_loc = 0
        b_dir = True
        for train_id in range(self.num_trains):
            tid = str(train_id).zfill(3)
            train = Train(
                f"{self.color.name[0].upper()}L{tid}", Train.status.in_service
            )
            trains.append(train)

            if b_dir:
                self.stations[curr_loc].arrive_b(train, None, None)
            else:
                self.stations[curr_loc].arrive_a(train, None, None)
            curr_loc, b_dir = self._get_next_idx(curr_loc, b_dir)

        return trains

    def run(self, timestamp: datetime, time_step: timedelta) -> None:
        """
        Advance the simulation by one step: run turnstiles, then move trains.

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
        self._advance_turnstiles(timestamp, time_step)
        self._advance_trains()

    def close(self) -> None:
        """
        Stop the simulation for this line by closing every station.

        Returns
        -------
        None
        """
        _ = [station.close() for station in self.stations]

    def _advance_turnstiles(self, timestamp: datetime, time_step: timedelta) -> None:
        """
        Run every station's turnstile for this simulation step.

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
        _ = [station.turnstile.run(timestamp, time_step) for station in self.stations]

    def _advance_trains(self) -> None:
        """
        Move every train on the line forward by one station.

        Starting from the first train in the 'b' direction, each train leaves
        its station and arrives at the next one, producing an arrival event
        that records the station and direction it came from.

        Returns
        -------
        None
        """
        # Find the first b train
        curr_train, curr_index, b_direction = self._next_train()
        self.stations[curr_index].b_train = None

        trains_advanced = 0
        while trains_advanced < self.num_trains - 1:
            # The train departs the current station
            if b_direction is True:
                self.stations[curr_index].b_train = None
            else:
                self.stations[curr_index].a_train = None

            prev_station = self.stations[curr_index].station_id
            prev_dir = "b" if b_direction else "a"

            # Advance this train to the next station
            curr_index, b_direction = self._get_next_idx(
                curr_index, b_direction, step_size=1
            )
            if b_direction is True:
                self.stations[curr_index].arrive_b(curr_train, prev_station, prev_dir)
            else:
                self.stations[curr_index].arrive_a(curr_train, prev_station, prev_dir)

            # Find the next train to advance
            move = 1 if b_direction else -1
            next_train, curr_index, b_direction = self._next_train(
                curr_index + move, b_direction
            )
            if b_direction is True:
                curr_train = self.stations[curr_index].b_train
            else:
                curr_train = self.stations[curr_index].a_train

            curr_train = next_train
            trains_advanced += 1

        # The last train departs the current station
        if b_direction is True:
            self.stations[curr_index].b_train = None
        else:
            self.stations[curr_index].a_train = None

        # Advance last train to the next station
        prev_station = self.stations[curr_index].station_id
        prev_dir = "b" if b_direction else "a"
        curr_index, b_direction = self._get_next_idx(
            curr_index, b_direction, step_size=1
        )
        if b_direction is True:
            self.stations[curr_index].arrive_b(curr_train, prev_station, prev_dir)
        else:
            self.stations[curr_index].arrive_a(curr_train, prev_station, prev_dir)

    def _next_train(
        self, start_index: int = 0, b_direction: bool = True, step_size: int = 1
    ) -> tuple[Train | None, int, bool]:
        """
        Find the next train from a starting index, in either direction.

        Searches in the given direction first; if no train is found, wraps
        around and searches the other direction from its far end.

        Parameters
        ----------
        start_index : int, optional
            Station index to start searching from. Defaults to ``0``.
        b_direction : bool, optional
            ``True`` to search the 'b' direction first, ``False`` for 'a'.
            Defaults to ``True``.
        step_size : int, optional
            Number of stations to move per search step. Defaults to ``1``.

        Returns
        -------
        tuple[Train | None, int, bool]
            The train found, its station index, and whether it is travelling
            in the 'b' direction.
        """
        if b_direction is True:
            curr_index = self._next_train_b(start_index, step_size)

            if curr_index == -1:
                curr_index = self._next_train_a(len(self.stations) - 1, step_size)
                b_direction = False
        else:
            curr_index = self._next_train_a(start_index, step_size)

            if curr_index == -1:
                curr_index = self._next_train_b(0, step_size)
                b_direction = True

        if b_direction is True:
            return self.stations[curr_index].b_train, curr_index, True
        return self.stations[curr_index].a_train, curr_index, False

    def _next_train_b(self, start_index: int, step_size: int) -> int:
        """
        Find the next station with a train in the 'b' direction.

        Parameters
        ----------
        start_index : int
            Station index to start searching from, moving towards the 'b' end.
        step_size : int
            Number of stations to move per search step.

        Returns
        -------
        int
            Index of the station holding the train, or ``-1`` if there is none.
        """
        for i in range(start_index, len(self.stations), step_size):
            if self.stations[i].b_train is not None:
                return i
        return -1

    def _next_train_a(self, start_index: int, step_size: int) -> int:
        """
        Find the next station with a train in the 'a' direction.

        Parameters
        ----------
        start_index : int
            Station index to start searching from, moving towards the 'a' end.
        step_size : int
            Number of stations to move per search step.

        Returns
        -------
        int
            Index of the station holding the train, or ``-1`` if there is none.
        """
        for i in range(start_index, 0, -step_size):
            if self.stations[i].a_train is not None:
                return i
        return -1

    def _get_next_idx(
        self, curr_index: int, b_direction: bool, step_size: int | None = None
    ) -> tuple[int, bool]:
        """
        Calculate the next station index, turning around at either end.

        Parameters
        ----------
        curr_index : int
            Current station index.
        b_direction : bool
            ``True`` if travelling in the 'b' direction, ``False`` for 'a'.
        step_size : int | None, optional
            Number of stations to move. Defaults to ``None``, which spaces the
            trains evenly along both directions of the line.

        Returns
        -------
        tuple[int, bool]
            The next station index, and whether the train is now travelling
            in the 'b' direction.
        """
        if step_size is None:
            step_size = int((self.num_stations * Line.num_directions) / self.num_trains)
        if b_direction is True:
            next_index = curr_index + step_size
            if next_index < self.num_stations:
                return next_index, True
            else:
                return self.num_stations - (next_index % self.num_stations), False
        else:
            next_index = curr_index - step_size
            if next_index > 0:
                return next_index, False
            else:
                return abs(next_index), True

    def __str__(self) -> str:
        """
        Format every station on the line, one per row.

        Returns
        -------
        str
            Each station's ``__str__`` output, joined by newlines.
        """
        return "\n".join(str(station) for station in self.stations)

    def __repr__(self) -> str:
        """
        Return the same representation as ``__str__``.

        Returns
        -------
        str
            The string produced by ``__str__``.
        """
        return str(self)
