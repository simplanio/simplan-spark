# Drop duplicates Transformation

Drop Duplicates is a transformation that allows you to remove duplicate rows from a a set of rows.

## Operator Definition

| Short Name | Fully Qualified Name |
|-----------|-------------|
| DropDuplicatesOperator | com.intuit.data.simplan.spark.core.operators.transformations.DropDuplicatesOperator |

## Compatability

| Batch | Streaming |
|-------|-----------|
| Yes | Yes |

## Configuration

```hocon
    DropDuplicateVendorEvents {
      action {
        operator = DropDuplicatesOperator
        config = {
          source = ProjectRequiredFields
          primaryKeyColumns = [fieldNamew]
          dropWindowDuration = "10 minutes"
        }
      }
    }
```

## Parameters
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Name of the source DataFrame. | Yes | NA |
| projections | List of columns to be selected. | Yes | NA |