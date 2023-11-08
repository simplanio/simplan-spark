# Json Batch Source

Reads Json records from a location set and converts each record into a Structured Record. The schema of the Structured Record is infered from the underlying data. Users can provide schema using the schema property. Schema is provided as [Schema Qualifed Param](../../../../qualified_param/schema_qualified_param.md).

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| JsonBatchSource | com.intuit.data.simplan.spark.core.operators.sources.batch.JsonBatchSource |


## Configuration

``` javascript
ReadData {
  action {
    operator = JsonBatchSource
    config = {
      tableType = <TEMP | MANAGED | NONE>
      schema = schemaJson("/path/to/schema.json")
      table = "table_name" 
      path = /path/to/file 
      options = {
        "option1" = "value1"
      }
    }
  }
}
```

## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| path | Path to the file or directory to read from. | Yes | NA |
| schema | Schema of the records in the file. | No | NA |
| tableType | Type of table that gets created. <br/> <strong>NONE</strong> - Maintained in memory as dataframe(Not usable in SQL)<br/> <strong>MANAGED</strong> - As managed table in Hive <br/> <strong>TEMP</strong> - Temperory table in Memory(Usable in SQL) | No | NONE |
| table | Name of the table to be created, if MANAGED is selected as tableType | No | {task-name} |
| options | Options to be passed to the reader. | No | NA |

### Default Options

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| inferSchema | Automatically infer data types. | No | true |