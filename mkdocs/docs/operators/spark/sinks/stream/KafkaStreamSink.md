# Kafka Stream Sink

The Kafka Stream Sink operator writes records to a Kafka Stream. 

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| KafkaStreamingSource | com.intuit.data.simplan.spark.core.operators.sources.stream.KafkaStreamingSource |


## Configuration

``` javascript
    ProduceToKafka {
      action {
        operator = KafkaStreamingSink
        config {
          source = SourceDataFrame
          outputMode = COMPLETE | UPDATE | APPEND
          options {
            "kafka.bootstrap.servers" = "localhost:9092"
            topic = topicName
            checkpointLocation: /path/to/checkpoint
          }
        }
      }
    }
```

## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Name of the source DataFrame. | Yes | NA |
| outputMode | Output mode of the stream. | Yes | NA |
| options | Options for the Kafka Stream. | Yes | NA |
