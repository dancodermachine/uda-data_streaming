# Stream Processing with Faust

## 1. Faust
* Built at Robinhood to tackle stream processing problems natively in Python.
* Faust takes its design inspiration from Kafka Streams, a JVM-only framework.
* Faust is built using modern Python features like asyncio, and requires Python 3.6 or higher.
* Faust does not natively support Avro or Schema Registry.

## 2. Serialization vs. Deserialization
```
Python object
    │
    │ serialize
    ▼
bytes / Kafka message
    │
    │ deserialize
    ▼
Python object
``` 
* Python `dataclass` objects can be marked as `frozen`, which makes them immutable - Nothing in Python is truly immutable, but this attribute gets you about as close as you can get
* Can be paired with the `asdict` function to quickly transform dataclasses into dictionaries

```python
@dataclass(frozen=True)
class Purchase:
    username: str = ""
    currency: str = ""
    amount: int = 0
```

### 2.1 Faust Deserialization - Key Points
* All data model classes must inherit from the `faust.Record` class if you wish to use them with a Faust topic.
* It is a good idea to specify the `serializer` type on your so that Faust will deserialize data in this format by default.
* It is a good practice to set `validation=True` on your data models. When set to true, Faust will enforce that the data being deserialized from Kafka matches the expected type. - E.g., if we expect a `str` for a field, but receive an `int`, Faust will raise an error.

```python
class Purchase(faust.Record, validation=True, serializer="json"):
    username: str
    currency: str
    amount: int
```

### 2.2 Faust Serialization - Key Points
* Serialization in Faust leverages the same `faust.Record` that we saw in the deserialization section. Faust runs the serializer in reverse to serialize the data for the output stream.
* Multiple serialization codecs may be specified for a given model - e.g., `serialization="binary|json"`. This means, when serializing, encode to binary first, then encode the binary data to json.

```python
class Purchase(faust.Record, validation=True, serializer="binary|json"):
    username: str
    currency: str
    amount: int
```

## 3. Storage in Faust
Faust stores its changelog in Kafka, and uses RocksDB for local state.
* Faust keeps track of all state changes in stateful tables by writing changelog events to dedicated changelog topics per table.
* This allows Faust to recover state and scale processing nodes without losing data.

How does this work?

When events update a stateful table, Faust emits changelog events to the corresponding changelog topic, ensuring fault tolerance and consistency.

In-Memory storage should only be used for test and local development.

Storing state in RocksDB.
* RocksDB is a highly performant datastore that runs side-by-side with a stream processing application.
* As changes are made in your streaming table, the state is stored in RocksDB in addition to being sent to Kafka.
* It is always recommended to use RocksDB in production.

Kafka is used by Faust to store change events for stateful applications, enabling reliable stream processing and state management by persisting state changes in Kafka topics.

## 4. Stream Processors & Operations
Faust provides the ability to provide pre-defined processor callbacks for data streams. Processors can add missing fields, change the meaning of fields, and perform any kind of desired processing.

* [Processors](https://faust.readthedocs.io/en/latest/userguide/streams.html#id2) are functions that take a value and return a value and can be added in a pre-defined list of callbacks to your stream declarations.
* Processors promote reusability and clarity in your code.
* Processors may execute synchronously or asynchronously within the context of your code.
* All defined processors will run, in the order they were defined, before the final value is generated.

```python
def transform(input_model):
    return transform(input_model)

async def delete_field(input_model):
    return delete_pii(input_model)

demo_stream = app.stream(
    demo_topic,
    processors=[transform, delete_field]
)
```
* The [`group_by`](https://faust.readthedocs.io/en/latest/userguide/streams.html#group-by-repartition-the-stream) operation ingests every incoming event from a source topic, and emits it to an intermediate topic with the newly specified key.
* The [`filter`](https://faust.readthedocs.io/en/latest/userguide/streams.html#filter-filter-values-to-omit-from-stream) operation uses a boolean function to determine whether or not a particular record should be kept or discarded. Any records that are kept are written to a new intermediate stream.
* The [`take`](https://faust.readthedocs.io/en/latest/userguide/streams.html#take-buffer-up-values-in-the-stream) operation bundles groups of events before invoking another iteration of the stream. Be careful to specify the `within` `datetime.timedelta` argument to this function, otherwise your program may hang.

Summary:
* **Processor** = the thing doing the processing.
* **Operation** = a particular transformation that the processor applies to the stream.
```
Kafka Topic
    │
    ▼
┌─────────────────────────────┐
│     Processor / Agent       │
│                             │
│  filter()    ← operation    │
│     ↓                       │
│  group_by()  ← operation    │
│     ↓                       │
│  map()       ← operation    │
└─────────────────────────────┘
    │
    ▼
Kafka Topic
```

## 5. Fause Tables
Faust provides an API for creating stateful applications with streaming Tables.
* Faust tables are defined with app.Table and take a table name and default type argument.
* Tables must be co-partitioned with the streams they are aggregating. Use the group_by operation to ensure co-partitioning.
* Tables which are not co-partitioned may lead to inaccurate results.

## 6. Windowing in Faust
Faust provides two windowing methods: hopping and tumbling. Windowing applies only to Tables.

**Tumbling**:
```python
table = app.Table("currency_total", int)

# Tumbling Window
tumbling_table = table.Tumbling(size=timedelta(minutes=5)) # Each window covers 5 minutes; a new window starts when the previous one ends
```
```
Window 1: 00:00 ───── 00:05
Window 2:               00:05 ───── 00:10
Window 3:                             00:10 ───── 00:15
```
**Hoping**:
```python
table = app.Table("currency_total", int)

# Hopping Window
hopping_table = table.Hopping(
    size=timedelta(minutes=5), # Each window covers 5 minutes of data
    step=timedelta(minutes=1), # Start a new window every 1 minute
    expires=timedelta(minutes=60) #  # Keep window data/state for 60 minutes before expiring it
)
```
```
Window 1:  00:00 ───────── 00:05
Window 2:    00:01 ───────── 00:06
Window 3:      00:02 ───────── 00:07
Window 4:        00:03 ───────── 00:08
                 ↑
              1 min step
```

## 7. Glossary
* **DSL - Domain Specific Language**. A metaprogramming language for specific tasks, such as building database queries or stream processing applications.
* **Dataclass** (Python) - A special type of Class in which instances are meant to represent data, but not contain mutating functions.
* **Changelog** - An append-only log of changes made to a particular component. In the case of Faust and other stream processors, this tracks all changes to a given processor.
* **Processor** (Faust) - A component that processes streams, typically implemented as agents or stream processing functions that consume and produce streams. They are defined to process incoming stream data and emit processed results.
* **Operations** (Faust) - Actions that can be applied to an incoming stream to create an intermediate stream containing some modification, such as a group-by or filter.