"""Contains functionality related to Stations"""
import json
import logging


logger = logging.getLogger(__name__)


class Station:
    """Defines the Station Model"""

    def __init__(self, station_id: int, station_name: str, order: int) -> None:
        """
        Create a station with no trains and no turnstile entries.

        Parameters
        ----------
        station_id : int
            CTA identifier of the station.
        station_name : str
            Human-readable station name.
        order : int
            Position of the station along its line.

        Returns
        -------
        None
        """
        self.station_id = station_id
        self.station_name = station_name
        self.order = order
        self.dir_a: dict[str, str] | None = None
        self.dir_b: dict[str, str] | None = None
        self.num_turnstile_entries = 0

    @classmethod
    def from_message(cls, value: dict) -> "Station":
        """
        Create a station from a Faust stations table record.

        Parameters
        ----------
        value : dict
            Record with ``station_id``, ``station_name`` and ``order``.

        Returns
        -------
        Station
            The new station.
        """
        return Station(value["station_id"], value["station_name"], value["order"])

    def handle_departure(self, direction: str) -> None:
        """
        Remove the train in the given direction from the station.

        Parameters
        ----------
        direction : str
            ``"a"`` or ``"b"``.

        Returns
        -------
        None
        """
        if direction == "a":
            self.dir_a = None
        else:
            self.dir_b = None

    def handle_arrival(self, direction: str, train_id: str, train_status: str) -> None:
        """
        Record a train arriving at the station.

        Parameters
        ----------
        direction : str
            ``"a"`` or ``"b"``.
        train_id : str
            Identifier of the arriving train, e.g. ``"BL001"``.
        train_status : str
            Train status, e.g. ``"in_service"``; shown with spaces.

        Returns
        -------
        None
        """
        status_dict = {"train_id": train_id, "status": train_status.replace("_", " ")}
        if direction == "a":
            self.dir_a = status_dict
        else:
            self.dir_b = status_dict

    def process_message(self, json_data: dict) -> None:
        """
        Update the turnstile entry count from a KSQL summary record.

        Parameters
        ----------
        json_data : dict
            ``TURNSTILE_SUMMARY`` record with ``STATION_ID`` and ``COUNT``.

        Returns
        -------
        None
        """
        self.num_turnstile_entries = json_data["COUNT"]
