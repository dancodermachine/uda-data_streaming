# Redis, Base64, and JSON
**Redis** is a database used primarily for chaching; this means it is optimised for fast reads and writes.

**Base64** is an encoding format used by computers to transmit and store information. It  is used to encode binary data into ASCII characters to ensure safe transmission over text-based protocols.
* Base64 is a system of representing binary data (8-bit data), in text format.
* Base64 encoding divides binary data into 6-bit groups, each represented by a character from a set of 64 characters.
* It can be used to represent any binary data, including text and photographs.
* Python/Spark:
    ```python
    unbase64(encodedStreamingDF.reservation).cast("string")
    ```

Spark SQL functions:
| Function | Purpose |
|----------|---------|
| `to_json` | Create JSON |
| `from_json` | Parse JSON |
| `unbase64` | Base64 Decode |
| `base64` | Base64 Encode |
| `split` | Separate a string of characters |

Code:
1. Manually Save and Read with Redis and Kafka
2. Parse Base64 Encoded Information
3. Sink a Subset of JSON

If you have a Spark streaming application sending data to Kafka, but you realise that you accidentally wrote to the wrong topic:
* Change the topic name the application writes to.
* Add the failOnDataLoss option like this: `.option("failOnDataLoss", false)`