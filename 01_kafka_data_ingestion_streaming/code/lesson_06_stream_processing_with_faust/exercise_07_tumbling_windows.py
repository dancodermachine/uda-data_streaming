from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import random
import faust

@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise7", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# Define a tumbling window of 10 seconds
# See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
#
uri_summary_table = app.Table("uri_summary", default=int).tumbling(
    timedelta(seconds=10)
)


@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Total ``number`` per URI over 10-second tumbling windows.

    Repartitions the stream by ``uri``, adds each event's ``number`` to the
    windowed ``uri_summary_table`` and prints the total for the window the
    event falls into. Windows follow the Kafka message timestamp, not the
    event's ``timestamp`` field.

    Parameters
    ----------
    clickevents : faust.StreamT[ClickEvent]
        Stream of click events consumed from
        ``com.udacity.streams.clickevents``.

    Returns
    -------
    None
        Runs until the worker is stopped.
    """
    async for ce in clickevents.group_by(ClickEvent.uri):
        uri_summary_table[ce.uri] += ce.number
        #
        # Play with printing value by: now(), current(), value()
        # See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
        #
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()