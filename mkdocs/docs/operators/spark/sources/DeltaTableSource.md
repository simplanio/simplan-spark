# Avro Batch Source

Reads Parquet records from a location set and converts each record into a Structured Record. The schema of the Structured Record is derived from the Parquet schema. If the Parquet schema is not available, it can be provided using the schema property. Schema is provided as [Schema Qualifed Param](../../../../qualified_param/schema_qualified_param.md).

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| DeltaTableSource | com.intuit.data.simplan.spark.core.operators.sources.DeltaTableSource |


## Configuration

``` javascript
ReadData {
  action {
    operator = DeltaTableSource
    config = {
      path = /path/to/file
      schema = schemaJson("/path/to/schema.json")
      autoCreate = true
    }
  }
}
```

## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| path | Path to the file or directory to read from. | Yes | NA |
| schema | Uses this schema to create table if table does not exist | No | NA |
| autoCreate | If true, creates the table if it does not exist. | No | true |