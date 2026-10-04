import faust

# Create the faust app with a name and broker
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
#
app = faust.App("hello-world-faust", broker="kafka://localhost:9092")

#
# Connect Faust to com.udacity.streams.clickevents
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
#
topic = app.topic("com.udacity.streams.clickevents")

#
# Provide an app agent to execute this function on topic event retrieval
# See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor
#
@app.agent(topic)
async def clickevent(clickevents: faust.StreamT) -> None:
    """
    Print every event from the click events topic.

    Faust runs this agent for the ``com.udacity.streams.clickevents``
    topic. No ``value_type`` is set on the topic, so each event is printed
    as Faust decoded it, without being turned into a model.

    Parameters
    ----------
    clickevents : faust.StreamT
        Stream of events consumed from the topic.

    Returns
    -------
    None
        Runs until the worker is stopped.
    """
    # Define the async for loop that iterates over clickevents
    # See: https://faust.readthedocs.io/en/latest/userguide/agents.html#the-stream
    # Print each event inside the for loop
    async for clickevent in clickevents:
        print(clickevent)

if __name__ == "__main__":
    app.main()