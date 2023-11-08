# File Streaming Source

Reads records from a File as a Stream and converts each record into a Structured Record. The schema of the Structured Record can be provided using the schema property. Schema is provided as [Schema Qualifed Param](../../../../qualified_param/schema_qualified_param.md).


## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| FileStreamingSource | com.intuit.data.simplan.spark.core.operators.sources.stream.FileStreamingSource |


## Configuration
``` javascript

    ReadStream {
      action {
        operator = FileStreamingSource
        config = {
          format = <JSON, AVRO, PARQUET, ORC, CSV, TEXT>
          schema = schemaJson("/path/to/schema.json")
          tableType = < NONE | TEMP>
          watermark = {
            eventTime = fieldName
            delayThreshold = "1 minute"
          }
          options = {
            <options1> = <value1>
          }
        }
      }
    }

```


## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| format | Format of the file. | Yes | NA |
| schema | Schema of the records in the file. | Yes | NA |
| tableType | Type of table that gets created. <br/> <strong>NONE</strong> - Maintained in memory as dataframe(Not usable in SQL)<br/> <strong>TEMP</strong> - Temperory table in Memory(Usable in SQL) | No | NONE |
| watermark | Watermark configuration. | No | NA |
| options | Options to be passed to the file reader. <br /> Any valid spark property can be passed depending on the file format you wish to read | No | NA |
