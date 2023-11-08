# Kafka Stream Reader

Reads records from a Kafka Stream and converts each record into a Structured Record. The schema of the Structured Record can be provided using the schema property. Schema is provided as [Schema Qualifed Param](../../../../qualified_param/schema_qualified_param.md). 

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| KafkaStreamingSource | com.intuit.data.simplan.spark.core.operators.sources.stream.KafkaStreamingSource |


## Configuration

``` javascript
    ReadVendorStream {
      action {
        operator = KafkaStreamingSource
        config = {
          format = JSON
          payloadSchema = schemaJson("path/to/payloadSchema.schema.json")
          headerSchema = schemaJson("path/to/headerSchema.schema.json")
          headerField = "key"
          tableType = <TEMP | NONE>
          watermark ={
            eventTime = fieldName
            delayThreshold = "1 minute"
          }
          parseMode = <ALL_PARSED | PAYLOAD_ONLY | HEADER_ONLY | ALL>
          options = {
            "kafka.bootstrap.servers" = "server1:port, server2:port"
            includeHeaders = true
            subscribe = topic-name
            ...
          }
        }
      }
    }
```

## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| format | Format of the data in the stream. | Yes | NA |
| payloadSchema | Schema of the payload in the stream. | Yes | NA |
| headerSchema | Schema of the header in the stream. | No | NA |
| headerField | Name of the field in the header schema that contains the header. | No | key |
| tableType | Type of table that gets created. <br/> <strong>NONE</strong> - Maintained in memory as dataframe(Not usable in SQL)<br/> <strong>TEMP</strong> - Temperory table in Memory(Usable in SQL) | No | NONE |
| watermark | Watermark configuration for the stream. | No | NA |
| parseMode | Mode of parsing the stream. <br/> <strong>ALL_PARSED</strong> - Both header and payload are parsed and available in the dataframe. <br/> <strong>PAYLOAD_ONLY</strong> - Only payload is parsed and available in the dataframe. <br/> <strong>HEADER_ONLY</strong> - Only header is parsed and available in the dataframe. <br/> <strong>ALL</strong> - Both header and payload are parsed and available in the dataframe. | No | PAYLOAD_ONLY |
| options | Options to be passed to the Kafka reader. <br /> Any Kafka property can be passed prefixed with kafka.* | Yes | NA |


## Sample Configuration

```hocon
    bands {
  action {
    operator = KafkaStreamReader
    config = {
      format = JSON
      payloadSchema = schemaJson("schema.json")
      headerSchema = schemaJson("header.schema.json")
      parseMode = ALL_PARSED
      options = {
        "kafka.bootstrap.servers" = "b-3.eventbusmsk-sbseg-e2e.hz7ee1.c3.kafka.us-west-2.amazonaws.com:9094, b-4.eventbusmsk-sbseg-e2e.hz7ee1.c3.kafka.us-west-2.amazonaws.com:9094, b-2.eventbusmsk-sbseg-e2e.hz7ee1.c3.kafka.us-west-2.amazonaws.com:9094"
        "kafka.cluster.env" = "e2e"
        "kafka.cluster.name" = "ebus_sbseg_sl"
        "kafka.cluster.region" = "uw2"
        "kafka.group.id" = "test111"
        "kafka.auto.commit.interval.ms" = "1000"
        "kafka.ssl.keystore.location" = "/Users/tabraham1/Intuit/Development/certifates/local-certificate.jks"
        "kafka.ssl.keystore.password" = "<password>"
        "kafka.security.protocol" = "ssl"
        "includeHeaders" = "true"
        subscribe = "e2e.qbo.commerce.vendormanagement.vendor.v1"
      }
    }
  }
}
```