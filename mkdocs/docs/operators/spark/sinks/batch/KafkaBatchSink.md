# Kafka Batch Sink

For writing to  Kafka  as a batch

```hocon
WriteData {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.sources.batch.KafkaBatchSink
    config = {
      source = <file or previous operator>
      schema = /path/to/schema.json
      options ={
        subscribe = <topic name>
        <option2> = <value2>
      }
    }
  }
}
```
All optional values supported by spark can be added.
Example for Kafka Batch Sink

```hocon
 finalOutput {
      action {
        operator = KafkaStreamingSink
        config {
          source = filteringOperation
          options {
            "kafka.bootstrap.servers" = "localhost:9092"
            topic = finalOutput
            checkpointLocation: /Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-spark/spark-launcher/src/main/resources/test/kafka_checkpoint
          }
        }
      }
    }
```