from dataclasses import asdict, dataclass
import json
import random
import faust

@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int

app = faust.App("exercise6", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# Define a uri summary table
# See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
#
uri_summary_table = app.Table("uri_summary", default=int)


@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Keep a running total of ``number`` per URI in a Faust table.

    Repartitions the stream by ``uri`` with ``group_by`` so each URI is
    always handled by the same worker, adds the event's ``number`` to that
    URI's entry in ``uri_summary_table`` and prints the new total. The
    table is backed by a changelog topic, so totals survive restarts.

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
    #
    # Group By URI
    # See: https://faust.readthedocs.io/en/latest/userguide/streams.html#group-by-repartition-the-stream
    #
    async for ce in clickevents.group_by(ClickEvent.uri):
        #
        # Use the URI as key, and add the number for each click event. Print the updated
        # entry for each key so you can see how the table is changing.
        # See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
        #
        uri_summary_table[ce.uri] += ce.number
        print(f"{ce.uri}: {uri_summary_table[ce.uri]}")


if __name__ == "__main__":
    app.main()