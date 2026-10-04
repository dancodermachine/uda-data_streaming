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


app = faust.App("exercise8", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# Define a hopping window of 1 minute with a 10-second step
# See: https://faust.readthedocs.io/en/latest/reference/faust.tables.table.html?highlight=hopping#faust.tables.table.Table.hopping
#
uri_summary_table = app.Table("uri_summary", default=int).hopping(
    size=timedelta(minutes=1),
    step=timedelta(seconds=10)
)


@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Total ``number`` per URI over 1-minute hopping windows.

    Repartitions the stream by ``uri`` and adds each event's ``number`` to
    the windowed ``uri_summary_table``. Windows are 1 minute long and start
    every 10 seconds, so each event counts towards 6 overlapping windows.
    Prints the total from ``current()``, which is the most recently started
    window containing the event.

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
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()