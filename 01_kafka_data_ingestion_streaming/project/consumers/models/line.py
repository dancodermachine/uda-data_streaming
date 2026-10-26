"""Contains functionality related to Lines"""
import json
import logging

from confluent_kafka import Message

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color: str) -> None:
        """
        Create a line.

        Parameters
        ----------
        color : str
            Line colour: ``"blue"``, ``"red"`` or ``"green"``. Any other
            value gets a white display colour.

        Returns
        -------
        None
        """
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations: dict[int, Station] = {}

    def _handle_station(self, value: dict) -> None:
        """
        Add a station to this line, if it belongs to it.

        Parameters
        ----------
        value : dict
            Transformed station record from the Faust stations table.

        Returns
        -------
        None
        """
        if value["line"] != self.color:
            return
        self.stations[value["station_id"]] = Station.from_message(value)

    def _handle_arrival(self, message: Message) -> None:
        """
        Update train locations from an arrival event.

        Removes the train from the station it came from, then records it at
        the station it arrived at.

        Parameters
        ----------
        message : Message
            Avro-decoded arrival message; ``message.value()`` is a dict.

        Returns
        -------
        None
        """
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station")
        else:
            logger.debug(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("unable to handle message due to missing station")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def process_message(self, message: Message) -> None:
        """
        Route a Kafka message to the handler for its topic.

        Parameters
        ----------
        message : Message
            Message from the Faust stations table, a station arrivals topic,
            or the KSQL turnstile summary topic.

        Returns
        -------
        None
        """
        if message.topic() == "org.chicago.cta.stations.table.v1":
            try:
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", message.value(), e)
        elif message.topic().startswith("org.chicago.cta.station.arrivals."):
            self._handle_arrival(message)
        elif message.topic() == "TURNSTILE_SUMMARY":
            json_data = json.loads(message.value())
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.debug("unable to handle message due to missing station")
                return
            station.process_message(json_data)
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic()
            )
