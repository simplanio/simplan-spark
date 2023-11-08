# Avro Batch Sink

For Writing to Batch Sink

```hocon
WriteData {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.sources.batch.AvroBatchSink
    config = {
      source = <file path or previoue operator>
      location = /output/path
      options ={
        <option1> = <value1>
      }
    }
  }
}
```

All options supported by spark can be used.  

Example for writing to Avro Batch Sink

```hocon
    finalOutput {
      action {
        operator = AvroBatchSink
        config = {
          source = SqlOperation
          location = ${simplan.variables.configBasePath}/output/consolidated/
        }
      }
    }
```