# Delta Table Merge Transformation

The Delta Table Merge transformation allows you to merge data from a Delta Lake table into another Delta Lake table. The transformation is based on the [Delta Table Merge](https://docs.delta.io/latest/delta-update.html#merge-examples) operation.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| DeltaStreamingSink | com.intuit.data.simplan.spark.core.operators.sinks.stream.DeltaStreamingSink |

## Configuration

```hocon
    deltaWrite {
      action {
        operator = DeltaStreamingSink
        config = {
          outputMode = append
          source = formatEvents
          forEachBatch {
            handler = com.intuit.data.simplan.spark.core.handlers.foreachbatch.DeltaMergeForEachBatchHandler
            config = {
              deltaTableSource = live_table
              deltaEventsSource = formatEvents
              deltaTableSourceAlias = "live_table"
              deltaEventsSourceAlias = "events"
              mergeCondition = "live_table.id = events.id"
              matchCondition = [
                {
                  "expression" = "update condition"
                  "action" = "UPDATE"
                },
                {
                  "expression" = "delete condition"
                  "action" = "DELETE"
                }
              ],
              notMatchCondition = [
                {
                  "expression" = "insert condition"
                  "action" = "INSERT"
                }
              ]
            }
          }
          options {
            path = /Users/tabraham1/Intuit/Development/WorkSpace/Simplan/Workspace/deltaTable
            checkpointLocation = /Users/tabraham1/Intuit/Development/WorkSpace/Simplan/Workspace/deltaTable/checkpoint
          }
        }
      }
    }
```