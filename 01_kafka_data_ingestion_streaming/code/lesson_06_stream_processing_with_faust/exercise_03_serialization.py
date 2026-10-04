from dataclasses import asdict, dataclass
import json
import faust

@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int

@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int

app = faust.App("exercise3", broker="kafka://localhost:9092")

clickevents_topic = app.topic(
    "com.udacity.streams.clickevents",
    value_type=ClickEvent
)

# Define an output topic for sanitized click events, without the user email
sanitized_topic = app.topic(
    "com.udacity.streams.clickevents.sanitized",
    key_type=str,
    value_type=ClickEventSanitized
)

@app.agent(clickevents_topic)
async def clickevent(clickevents: faust.StreamT[ClickEvent]) -> None:
    """
    Forward each click event to the sanitized topic without the email.

    Builds a ``ClickEventSanitized`` from every incoming ``ClickEvent``,
    dropping the ``email`` field, and sends it to
    ``com.udacity.streams.clickevents.sanitized`` keyed by its ``uri``.

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
    async for clickevent in clickevents:
        #
        # Modify the incoming click event to remove the user email.
        # Create and send a ClickEventSanitized object.
        #
        sanitized = ClickEventSanitized(
            timestamp=clickevent.timestamp,
            uri=clickevent.uri,
            number=clickevent.number
        )
        #
        # Send the data to the topic you created above.
        # Make sure to set a key and value
        #
        await sanitized_topic.send(key=sanitized.uri, value=sanitized)

if __name__ == "__main__":
    app.main()


"""
Kafka
│
│  com.udacity.streams.clickevents
│
▼
ClickEvent
{
  email,
  timestamp,
  uri,
  number
}
│
│  Faust agent consumes it
│
│  removes email
▼
ClickEventSanitized
{
  timestamp,
  uri,
  number
}
│
│  sanitized_topic.send(...)
▼
Kafka
    com.udacity.streams.clickevents.sanitized
"""