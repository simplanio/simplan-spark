# Filtering Transformation

Filtering is a transformation that allows you to filter out rows from a DataFrame based on a condition. The condition can be a SQL expression which leads to a boolean outcome.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| FilteringOperator | com.intuit.data.simplan.spark.core.operators.transformations.FilteringOperator |

## Compatability

| Batch | Streaming |
|-------|-----------|
| Yes | Yes |

## Configuration

```hocon
 Filtering {
      action {
        operator = FilteringOperator
        config = {
          source = SourceDataFrame
          condition = "event_type = 'EventToBeFiltered'"
        }
      }
    }
```
## Parameters
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Name of the source DataFrame. | Yes | NA |
| condition | SQL expression that leads to a boolean outcome. | Yes | NA |
