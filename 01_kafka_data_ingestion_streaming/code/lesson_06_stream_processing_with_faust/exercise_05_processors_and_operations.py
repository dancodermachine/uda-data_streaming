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
    score: int = 0

#
# Define a scoring function for incoming ClickEvents.
# It doens't matter _how_ you score the incoming records, just perform
# some modification of the `ClickEvent.score` field and return the value
#
def add_score(clickevent: ClickEvent) -> ClickEvent:
    """
    Set a random score on a click event.

    Used as a stream processor, so Faust calls it on every event before the
    agent's loop receives it. It modifies the event in place and returns it.

    Parameters
    ----------
    clickevent : ClickEvent
        Incoming click event.

    Returns
    -------
    ClickEvent
        The same event, with ``score`` set to a random value from 0 to 100.
    """
    clickevent.score = random.randint(0, 100)
    return clickevent


app = faust.App("exercise5", broker="kafka://localhost:9092")
# Messages in this topic should have their values interpreted as ClickEvent objects.
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)
scored_topic = app.topic(
    "com.udacity.streams.clickevents.scored",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Score each click event and forward it to the scored topic.

    Registers ``add_score`` as a processor on the stream, so every event is
    scored before the loop sees it, then sends each scored event, keyed by
    its ``uri``, to ``com.udacity.streams.clickevents.scored``.

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
    # Add the `add_score` processor to the incoming clickevents
    # See: https://faust.readthedocs.io/en/latest/reference/faust.streams.html?highlight=add_processor#faust.streams.Stream.add_processor
    
    clickevents.add_processor(add_score)
    async for ce in clickevents:
        await scored_topic.send(key=ce.uri, value=ce)

if __name__ == "__main__":
    app.main()