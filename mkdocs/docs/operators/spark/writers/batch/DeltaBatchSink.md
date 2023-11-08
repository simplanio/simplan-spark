# Delta Batch Sink

For Writing to Batch Sink

```hocon
ReadData {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.sources.batch.DeltaBatchSink
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

Example for writing to Delta Batch Sink

```hocon
    finalOutput {
      action {
        operator = DeltaBatchSink
        config = {
          source = SqlOperation
          location = ${simplan.variables.configBasePath}/output/consolidated/
        }
      }
    }
```