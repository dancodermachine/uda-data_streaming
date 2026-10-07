# KSQL
KSQL provides a SQL-like interface to transform Kafka Topics into streams and tables.

* Show topics
    ```
    SHOW TOPICS
    ```

## 1. Stream Creation
There are two options for creating streams in KSQL:
1. Define the stream attributes and types with a create statement.
    ```sql
    CREATE STREAM purchases (
        username VARCHAR,
        currency VARCHAR,
        amount INT
    ) WITH (
        KAFKA_TOPIC = 'purchases',
        VALUE_FORMAT = 'JSON'
    );
    ```
2. Create a stream with a select that excludes or modifies data from another stream.
    ```sql
    CREATE STREAM purchases_high_value AS
    SELECT *
    FROM purchases
    WHERE amount > 100000;
    ```

Creating Streams from an underlying topic requires you to specify column names and their types
* You must also specify the serialization format as one of `JSON`, `AVRO`, or `DELIMITED` (csv).
* You must also specify the underlying topic name.
* You may create a stream from another existing stream with `CREATE STREAM <stream_name> AS SELECT …`
* Viewing available streams
    ```
    SHOW STREAMS
    ```
* Drop stream
    ```
    DROP STREAM popular_uris;
    ```

## 2. Creating a Table
* Creating Tables from an underlying topic requires you to specify column names and their types.
* You must also specify the serialization format as one of `JSON`, `AVRO`, or `DELIMITED` (csv).
* You must also specify the underlying topic name.

```sql
CREATE TABLE users (
    username VARCHAR,
    address VARCHAR,
    email VARCHAR,
    phone_number VARCHAR
) WITH (
    KAFKA_TOPIC = 'purchases',
    VALUE_FORMAT = 'JSON',
    KEY = 'username'
);
```
* Deleting Table
    ```
    SHOW QUERIES
    ```
    ```
    TERMINATE <query_name>
    ```
    ```
    DROP <table_name>
    ```
## 3. Querying
* `SELECT` statements may be run in KSQL CLI, but as soon as the session is terminated, so too is the data calculation.
* Use `CREATE STREAM <stream_name> AS SELECT…` and `CREATE TABLE <table_name> AS SELECT …` to persist your queries for long-term usage.
    ```sql
    SELECT username
    FROM purchases
    WHERE currency = 'USD'
    AND amount > 100000;
    ```

## 4. Windowing
* KSQL supports Tumbling windows with the `WINDOW TUMBLING (SIZE <duration>)` syntax.
    ```sql
    SELECT currency, SUM(amount)
    FROM purchase
    WINDOW TUMBLING (SIZE 10 MINUTES)
    GROUP BY currency;
    ```
* KSQL supports Hopping windows with the `WINDOW HOPPING (SIZE <duration>, ADVANCE BY <interval>)` syntax.
    ```sql
    SELECT currency, SUM(amount)
    FROM purchase
    WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE)
    GROUP BY currency;
    ```
* Session windowing: A window with a variable duration that varies depending on the presence of a specific key value.
    - Keeps track of differences between the time a key was last seen and the current key arrival time.
    - If the difference between the time a key was last seen and the current key arrival time, for two records with the same key, is larger than the session window length defined, a new window is started.
    - If the difference between the time a key was last seen and the current key arrival time, for two records with the same key, is less than the session window length, the record is added to the current window, and the session expiration time is started anew.
        ```sql
        CREATE TABLE clickevents_session AS
        SELECT uri FROM clickevents
        WINDOW SESSION (5 MINUTES)
        WHERE uri LIKE 'http://www.b%'
        GROUP BY uri;
        ```

## 5. Aggregating Data
* Use `GROUP BY` to create aggregations in KSQL.
* `GROUP BY` always creates a KSQL Table.
* KSQL supports aggregations like `COUNT`, `MAX`, `MIN`, `SUM`, `TOPK`, `HISTOGRAM` and more.

```sql
SELECT currency, SUM(amount)
FROM purchase
WINDOW SESSION (30 MINUTES)
GROUP BY currency;
```

## 6. Joins
* The `KEY` determines if the entities we wish to join are co-partitioned.
* The `KEY` used in KSQL and the Kafka Message must match.
* You can join:
    - Table to table
    - Stream to stream
    - Stream to table (left or inner)
* You can't:
    - Table to stream
    - Stream to table (outer)


```sql
SELECT p.username, p.amount, u.email
FROM purchase p
JOIN users u ON p.username = u.username;
```

## 7. Glossary
* **Kafka Streams** - A Java library for constructing stream processing applications. KSQL translates SQL statements to Kafka Streams applications.
* **User Defined Function (UDF)** - An extension to the SQL capabilities of KSQL written by the user. For KSQL, these are written in Java.
* **Key (KSQL)** - Data that uniquely identifies the value contained in this data message relative to other pieces of data in the stream. For example, a `user_id` may uniquely identify a `user` object.
* **Session Windowing (KSQL)** - A system that keeps track of when the last time a particular key was seen. When a new record with the same key arrives, the difference between the timestamps is calculated. If the difference is larger than a defined session window, then a new window is started for that session. If the difference is less than the defined session window, the new record is added to the existing window.