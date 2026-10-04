from dataclasses import asdict, dataclass
import json
import faust

@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int

app = faust.App("exercise4", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)
popular_uris_topic = app.topic(
    "com.udacity.streams.clickevents.popular",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Forward popular click events to the popular URIs topic.

    Filters the stream down to events whose ``number`` is at least 100 and
    sends each one, keyed by its ``uri``, to
    ``com.udacity.streams.clickevents.popular``.

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
    # Filter clickevents to only those with a number higher than or
    # equal to 100
    # See: https://faust.readthedocs.io/en/latest/userguide/streams.html#filter-filter-values-to-omit-from-stream
    #
    async for clickevent in clickevents.filter(lambda x: x.number >= 100):
        #
        # Send the message to the `popular_uris_topic` with a key and value.
        await popular_uris_topic.send(key=clickevent.uri, value=clickevent)
    pass

if __name__ == "__main__":
    app.main()