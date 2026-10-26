"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os

import requests

import topic_check


logger = logging.getLogger(__name__)


# ksqlDB on Confluent Cloud, authenticated with its own API key
KSQL_URL = os.environ["KSQLDB_ENDPOINT"]
KSQL_AUTH = (os.environ["KSQLDB_API_KEY"], os.environ["KSQLDB_API_SECRET"])

# The turnstile producer must write to this topic
TURNSTILE_TOPIC = "org.chicago.cta.turnstile"

# `turnstile` is a STREAM rather than a TABLE: every message is a separate rider entry, and
# ksqlDB tables keep only the latest row per key, which would undercount.
# Its columns are read from the turnstile value schema in Schema Registry.
#
# ksqlDB moves GROUP BY columns into the message key, but the consumer reads STATION_ID from
# the JSON value, so AS_VALUE copies it back into the value.
KSQL_STATEMENT = f"""
CREATE STREAM turnstile WITH (
    KAFKA_TOPIC='{TURNSTILE_TOPIC}',
    VALUE_FORMAT='AVRO'
);

CREATE TABLE turnstile_summary
WITH (KAFKA_TOPIC='TURNSTILE_SUMMARY', VALUE_FORMAT='JSON') AS
    SELECT
        station_id AS station_key,
        AS_VALUE(station_id) AS station_id,
        COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement() -> None:
    """
    Create the ksqlDB turnstile stream and per-station summary table.

    Does nothing if the ``TURNSTILE_SUMMARY`` topic already exists, since
    that means the statements have already been run.

    Returns
    -------
    None

    Raises
    ------
    requests.HTTPError
        If ksqlDB rejects the statements.
    """
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        auth=KSQL_AUTH,
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error("ksqlDB rejected the statements: %s", resp.text)
        raise


if __name__ == "__main__":
    execute_statement()
