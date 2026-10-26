"""Defines trends calculations for stations"""
import logging
import os
import ssl

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    """Raw station row, as written to Kafka by the JDBC source connector"""

    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    """Trimmed station record with the line colour as a single field"""

    station_id: int
    station_name: str
    order: int
    line: str


# Confluent Cloud requires SASL/SSL auth on top of the broker address
app = faust.App(
    "stations-stream",
    broker=f'kafka://{os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]}',
    broker_credentials=faust.SASLCredentials(
        username=os.environ["CONFLUENT_KAFKA_KEY"],
        password=os.environ["CONFLUENT_KAFKA_SECRET"],
        ssl_context=ssl.create_default_context(),
    ),
    # Confluent Cloud requires a replication factor of 3 for every topic Faust creates
    topic_replication_factor=3,
    # A single worker needs no leader election, so skip its extra internal topic
    topic_disable_leader=True,
    store="memory://",
)
# Kafka Connect writes the `stations` table to `<topic.prefix><table>`
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# The consumer server reads the transformed stations from this topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
# Every write to the table is published to its changelog, which is the output topic
table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


def station_line(station: Station) -> str:
    """
    Get the colour of the line a station belongs to.

    Parameters
    ----------
    station : Station
        Raw station record from the connector.

    Returns
    -------
    str
        ``"red"``, ``"blue"`` or ``"green"``, or ``"other"`` if the station is
        on none of those lines (e.g. Brown or Pink line stops).
    """
    if station.red:
        return "red"
    if station.blue:
        return "blue"
    if station.green:
        return "green"
    return "other"


@app.agent(topic)
async def transform_station(stations: faust.StreamT[Station]) -> None:
    """
    Transform raw stations into ``TransformedStation`` records.

    Keeps only the fields the consumer needs, turns the ``red``/``blue``/
    ``green`` flags into a single ``line`` string, and stores the result in
    ``table`` keyed by ``station_id``, which publishes it to
    ``org.chicago.cta.stations.table.v1``. Stations on other lines get
    ``line="other"``, so every station ID is present; the consumer ignores
    them, since the simulation only models red, blue and green. An
    ``"other"`` stop never replaces a station already stored with a real
    line.

    Parameters
    ----------
    stations : faust.StreamT[Station]
        Stream of raw station records from ``org.chicago.cta.stations``.

    Returns
    -------
    None
        Runs until the worker is stopped.
    """
    async for station in stations:
        line = station_line(station)
        # A station ID can have stops on a simulated line and stops only on other lines;
        # never let an "other" stop replace the line the consumer needs
        if line == "other" and station.station_id in table:
            continue

        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )


if __name__ == "__main__":
    app.main()
