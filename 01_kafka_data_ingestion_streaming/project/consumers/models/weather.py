"""Contains functionality related to Weather"""
import logging

from confluent_kafka import Message


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self) -> None:
        """
        Create the weather model with default values until the first event arrives.

        Returns
        -------
        None
        """
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message: Message) -> None:
        """
        Update the current weather from a weather event.

        Parameters
        ----------
        message : Message
            Avro-decoded weather message; ``message.value()`` is a dict with
            ``temperature`` and ``status``.

        Returns
        -------
        None
        """
        value = message.value()
        self.temperature = value["temperature"]
        self.status = value["status"]
