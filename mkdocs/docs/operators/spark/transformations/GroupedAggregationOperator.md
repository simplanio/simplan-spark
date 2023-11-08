# Grouped Aggregation Transformation

Grouped aggregation is a transformation that allows you to aggregate rows from a DataFrame based on a group by condition. Expressions can be used to aggregate the rows. Any expressions supported by Spark SQL can be used.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| GroupedAggregationOperator | com.intuit.data.simplan.spark.core.operators.transformations.GroupedAggregationOperator |

## Compatability

| Batch | Streaming |
|-------|-----------|
| Yes | Yes |

## Configuration

```hocon
    GroupedAggregation {
      action {
        operator = GroupedAggregationOperator
        config = {
          source = sourceDataFrame
          grouping = [groupField]
          aggs {
            aggField1 = aggregationExpression1
            aggField2 = aggregationExpression2
          }
        }
      }
    }
```
## Parameters
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Name of the source DataFrame. | Yes | NA |
| grouping | List of fields to group by. | Yes | NA |
| aggs | Map of fields to aggregate and the aggregation expression. | Yes | NA |

## Example

Calculating 2 aggregations to find `numberjobs` and `totalNumberOfStatements` grouped by a field called `asset`.

```hocon
    NumberOfVendorsLifetime {
      action {
        operator = GroupedAggregationOperator
        config = {
          source = ProjectRequiredFields
          grouping = [asset]
          aggs {
            numberOfJobs = count(appName)
            totalNumberOfStatemts = sum(numOfStatements)
          }
        }
      }
    }
```