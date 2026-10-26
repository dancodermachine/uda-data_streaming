from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)


# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date in sql calculations, we are going
# to cast them as strings
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerJSONSchema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType()),
        StructField("accountNumber",StringType()),
        StructField("location",StringType())        
    ]
)

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]   
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("customer-record").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as redisEvent from RedisData")

# Here we are base64 decoding the redisEvent
zSetDecodedEntriesStreamingDF1= zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))
zSetDecodedEntriesStreamingDF2= zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))

# Filter DF1 for only those that contain the birthDay field (customer record)

zSetDecodedEntriesStreamingDF1.filter(col("redisEvent").contains("birthDay"))

# Filter DF2 for only those that do not contain the birthDay field (all records other than customer) we will filter out null rows later
zSetDecodedEntriesStreamingDF2.filter(~col("redisEvent").contains("birthDay"))


# Now we are parsing JSON from the redisEvent that contains customer record data
zSetDecodedEntriesStreamingDF1\
    .withColumn("customer", from_json("redisEvent", customerJSONSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")\

# Last we are parsing JSON from the redisEvent that contains customer location data
zSetDecodedEntriesStreamingDF2\
    .withColumn("customerLocation", from_json("redisEvent", customerLocationSchema))\
    .select(col('customerLocation.*'))\
    .createOrReplaceTempView("CustomerLocation")\

# Let's use some column aliases to avoid column name clashes
customerStreamingDF = spark.sql("select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null")

# We parse the birthdate to get just the year, that helps determine age
relevantCustomerFieldsStreamingDF = customerStreamingDF.select('customerAccountNumber','homeLocation',split(customerStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# Let's use some more column alisases on the customer location 
customerLocationStreamingDF = spark.sql("select accountNumber as locationAccountNumber, location from CustomerLocation")

currentAndHomeLocationStreamingDF = customerLocationStreamingDF.join(relevantCustomerFieldsStreamingDF, expr( """
   customerAccountNumber=locationAccountNumber
"""
))

# This takes the stream and "sinks" it to the console as it is updated one message at a time:

# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            735944113|      Syria|            735944113|       Syria|     1952|
# |            735944113|      Syria|            735944113|       Syria|     1952|
# |             45952249|New Zealand|             45952249| New Zealand|     1939|
# |            792107998|     Uganda|            792107998|      Uganda|     1951|
# |            792107998|     Uganda|            792107998|      Uganda|     1951|
# |            212014318|     Mexico|            212014318|      Mexico|     1941|
# |             87719362|     Jordan|             87719362|      Jordan|     1937|
# |            792350429|    Nigeria|            792350429|     Nigeria|     1949|
# |            411299601|Afghanistan|            411299601| Afghanistan|     1950|
# |            947563502|     Mexico|            947563502|      Mexico|     1944|
# |            920257090|      Syria|            920257090|       Syria|     1948|
# |            723658544|       Iraq|            723658544|        Iraq|     1943|
# |             39252304|    Alabama|             39252304|     Alabama|     1965|
# |             39252304|    Alabama|             39252304|     Alabama|     1965|
# |            508037708|     Brazil|            508037708|      Brazil|     1947|
# +---------------------+-----------+---------------------+------------+---------+
currentAndHomeLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
