"""Defines CTA Train Model"""
from enum import IntEnum
import logging


logger = logging.getLogger(__name__)


class Train:
    """Defines CTA Train Model"""

    status = IntEnum("status", "out_of_service in_service broken_down", start=0)

    def __init__(self, train_id: str, status: IntEnum | None) -> None:
        """
        Initialize a train.

        Parameters
        ----------
        train_id : str
            Train identifier, e.g. ``"BL001"``.
        status : IntEnum | None
            Starting status (a ``Train.status`` member). ``None`` is treated
            as ``Train.status.out_of_service``.

        Returns
        -------
        None
        """
        self.train_id = train_id
        self.status = status
        if self.status is None:
            self.status = Train.status.out_of_service

    def __str__(self) -> str:
        """
        Describe the train and its status in plain words.

        Returns
        -------
        str
            For example ``"Train ID BL001 is in service"``.
        """
        return f"Train ID {self.train_id} is {self.status.name.replace('_', ' ')}"

    def __repr__(self) -> str:
        """
        Return the same description as ``__str__``.

        Returns
        -------
        str
            The string produced by ``__str__``.
        """
        return str(self)

    def broken(self) -> bool:
        """
        Check whether the train is broken down.

        Returns
        -------
        bool
            ``True`` if the status is ``Train.status.broken_down``, ``False``
            otherwise.
        """
        return self.status == Train.status.broken_down
