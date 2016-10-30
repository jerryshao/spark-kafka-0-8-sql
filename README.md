# spark-kafka-0-8-sql

Spark Structured Streaming Kafka Source for Kafka 0.8.

This library is design for Spark Structured Streaming Kafka source, its aim is to provide equal functionalities for users who still use Kafka 0.8/0.9.

The main differences compared to Kafka 0.10 source are:

1. This Kafka 0.8 source uses `SimpleConsumer` rather than new `Consumer` API.
2. Some configurations (especially the name) are changed in Kafka 0.10, and here we still keep the conventions of Kafka 0.8.
3. We don't rewrite the whole Kafka connection logics compared to Kafka 0.10 source, instead we still use the existing implementations of Spark Streaming Kafka 0.8 direct approach.

## To Use It

Like other Sources in Spark ecosystem, the simplest way to use is to add the dependencies to Spark by:

```
spark-submit
  --master local[*] \
  --packages com.hortonworks.spark:spark-kafka-0-8-sql_2.11:1.0 \
  yourApp
  ...
```

Spark will automatically search central and local maven repositories to add dependencies to Spark runtime. Besides you coud use `mvn install` to publish this library to local Maven repo and use `--packages`, which will search local maven repo also.

To use `KafkaSource`, it is the same as any other Structured Streaming Sources already supported in Spark:

```scala

    import spark.implicits

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingoffset", "smallest")
      .option("topics", topic)

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .trigger(ProcessingTime(2000L))
      .start()

    kafka.awaitTermination()

```

### To compile

This Structured Streaming Kafka 0.8 source is built with Maven, you could build with:

```
mvn clean package
```

### Compactible Spark Version

Due to the rigid changes of Structured Streaming component, This Kafka 0.8 Source can only worked with Spark after 2.0.2 and master branch.

### Important notes:

1. The schema of Kafka 0.8 source is fixed, you cannot change the schema of Kafka 0.8 source, this is different from most of other Sources in Spark.

    ```scala

        StructType(Seq(
        StructField("key", BinaryType),
        StructField("value", BinaryType),
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType)))

    ```
2. You have to set `kafka.bootstrap.servers` or `kafka.metadata.broker` in Source creation.
3. You have to specify "topics" in Kafka 0.8 Source options, multiple topics are separated by ":".
4. All the Kafka related configurations set through Kafka 0.8 Source should be start with "kafka." prefix.
5. Option "startingoffset" can only be "smallest" or "largest".

# License

Apache License, Version 2.0 [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)
