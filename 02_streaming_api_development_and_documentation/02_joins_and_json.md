# Joins & JSON
## 1. JSON Payload
```json
{
  "truckNumber": "5169",
  "destination": "Florida",
  "milesFromShop": 505,
  "odometerReading": 50513
}
```
`StructType`: Using an array of `StructField`s, you create your `StructType`. Each `StructField` correlates with a JSON field.

`StructField` is a Python class used to create a typed field in a `StructType` (e.g. `StructField("truckNumber", StringType())`).
* The field name needs to match your JSON field
* Make sure to define the data type

```py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json

# Sample JSON: 
# {
#    "truckNumber":"2352523",
#    "destination":"Accra",
#    "milesFromShop":100,
#    "odometerReading": 99789
# }
truckStatusSchema = StructType (   
        [
          StructField("truckNumber", StringType()),        
          StructField("destination", StringType()), 
          StructField("milesFromShop", IntegerType()),  
          StructField("odometerReading", IntegerType())
        ]
) 

# Assuming vehicleStatusStreamingDF is defined elsewhere in the context
# For example: vehicleStatusStreamingDF = spark.readStream.format("kafka")... 
vehicleStatusStreamingDF \
  .withColumn("value", from_json("value", truckStatusSchema)) \ # converts the incoming JSON text into structured data
  .select("value.*") \ # separates it into individual columns
  .writeStream \ # processes the incoming data continuously
  .outputMode("append") \ # outputs only new records
  .format("console") \ # displays the results on the screen
  .start() \ # begins the streaming process
  .awaitTermination() # keeps the program waiting while the stream run     
```

## 2. Join Streaming Dataframes from Different Datasources
**Data Mocking**: The process of creating simulated or faked data that mimics the structure and characteristics of real data, used for testing or development when real data is unavailable or impractical to use.

**Join**: To connect two data collections by referencing a field they share in common; or the state which connects 2 data collections by referencing a common field.

```py
from pyspark.sql.functions import expr

# Assuming vehicleStatusSelectStarDF and vehicleCheckinSelectStarDF are defined
checkinStatusDF = vehicleStatusSelectStarDF \
.join(vehicleCheckinSelectStarDF, expr("""
    statusTruckNumber = checkinTruckNumber
"""                                                                             
))
```

## 3. Sink to Kafka
1. Create DataFrame
2. Identify the Kafka Topic
3. Sink to the Topic

```py
from pyspark.sql.functions import to_json, struct

# Assuming vehicleStatusDF is defined elsewhere in the context
vehicleStatusDF\
 .selectExpr("cast(statusTruckNumber as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "vehicle-status-changes")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
```