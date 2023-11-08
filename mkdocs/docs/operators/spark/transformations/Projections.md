# Projections Transformation

Projections is a transformation that allows you to select a subset of columns from a DataFrame. The columns can be selected by name or by using a SQL expression.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| ProjectionOperator | com.intuit.data.simplan.spark.core.operators.transformations.ProjectionOperator |

## Compatability

| Batch | Streaming |
|-------|-----------|
| Yes | Yes |

## Configuration

```hocon
    ProjectRequiredFields {
      action {
        operator = ProjectionOperator
        config = {
          source = SourceDataFrame
          projections = [
            "value.id.accountId as accountId",
            "value.id.entityId as entityId",
            "value.active as active",
            "headers.entityChangeAction as entityChangeAction",
            "headers.idempotenceKey as idempotenceKey",
            "current_timestamp() as timestamp"
          ]
        }
      }
    }
```

## Parameters
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Name of the source DataFrame. | Yes | NA |
| projections | List of columns to be selected. | Yes | NA |