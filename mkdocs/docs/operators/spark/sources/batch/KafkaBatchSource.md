# Kafka Batch Source

|  ⚠️ This operator is outdated. <br />This will be replaced with an operator that works similar to how Streaming Kafka Source structures data <br /> USE WITH CAUTION|
| --- |


For Reading Kafka data as a batch

```hocon
ReadData {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.sources.batch.KafkaBatchSource
    config = {
      schema = /path/to/schema.json
      options ={
        subscribe = <topic name>
      }
    }
  }
}
```


All kafka Options are supported in kafka batch source.

Example for KafkaBatchSource

```hocon
  finalOutput {
      action {
        operator = KafkaBatchSource
        config {
          schema = /path/to/schema.json
          options {
            "kafka.bootstrap.servers" = "localhost:9092"
            topic = finalOutput
            checkpointLocation: /Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/SimPlan/spark-launcher/src/main/resources/test/kafka_checkpoint
          }
        }
      }
    }
```