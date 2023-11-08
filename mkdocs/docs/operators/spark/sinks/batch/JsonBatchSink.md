# Json Batch Sink

For Reading Json Sink

```hocon
ReadData {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.sources.batch.JsonBatchSink
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

All options supported by spark can be used  

Example for writing to JsonBatch Sink

```hocon
    finalOutput {
      action {
        operator = JsonBatchSink
        config = {
          source = projectionOperation
          location = ${simplan.variables.configBasePath}/output/consolidated
        }
      }
    }
```