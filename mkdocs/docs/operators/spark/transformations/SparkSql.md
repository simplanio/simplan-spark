# SQL Transformations

SqlStatementExecutor is a Spark SQL transformation that executes a Spark SQL statement on a DataFrame. It is a wrapper around the Spark SQL API. Any Spark SQL statement can be executed using this transformation. Any existing table in Hive metastore can be queried using this transformation. The transformation can also be used to create a new table in Hive metastore.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| SqlStatementExecutor | com.intuit.data.simplan.spark.core.operators.transformations.SqlStatementOperator |

## Compatability

| Batch | Streaming |
|-------|-----------|
| Yes | Yes |

## Configuration
``` hocon
    TaskName {
      action {
        operator = SqlStatementExecutor
        config = {
            table = "table_name"
            tableType = <TEMP | MANAGED | NONE>
            sql = "SELECT field FROM schema.table_name"
        }
      }
    }
```

## Parameters
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| table | Name of the table to be created. | No | NA |
| tableType | Type of table that gets created. <br/ > NONE - Maintained in memory as dataframe(Not usable in SQL)<br /> MANAGED - As managed table in Hive <br /> TEMP - Temperory table in Memory(Usable in SQL) | No | NONE |
| sql | Spark SQL statement to be executed. | Yes | NA |
