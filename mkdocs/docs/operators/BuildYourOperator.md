# Build Your Operator

## Overview
Simplan framework is built around the concept of operators. An operator is intended to perform an operation which is a unit of work the framework can perform. Learn more about operators in the [Operators](https://github.intuit.com/pages/Simplan/simplan-framework/Operators/) section.

In this section, we will learn how to build an operator for SparkAppContext. We will build an operator that will execute a Spark SQL statement on a DataFrame. It is a wrapper around the Spark SQL API. Any Spark SQL statement can be executed using this transformation. Any existing table in Hive metastore can be queried using this transformation. The transformation can also be used to create a new table in Hive metastore.

If you just like to see the code, you can find it [here](https://github.intuit.com/Simplan/simplan-spark/blob/master/spark-core/src/main/scala/com/intuit/data/simplan/spark/core/operators/transformations/FilteringOperator.scala).

More examples of operators can be found [here](https://github.intuit.com/Simplan/simplan-spark/tree/master/spark-core/src/main/scala/com/intuit/data/simplan/spark/core/operators).

## Defining a configuration class
The config class is used to define the configuration parameters for the operator. A case class can be defined in a specific structure to emulate the configuration parameters. 

``` scala
case class WhereConditionOperatorConfig(source: String, condition: String)
```
| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| source | Source on which the filteration is to be performed | Yes | NA |
| condition | Condition to Filter | Yes | NA |

## Defining the operator
To create an operator targetted for SparkAppContext in Simplan you need to extend [SparkOperator](https://github.intuit.com/Simplan/simplan-spark/blob/master/spark-core/src/main/scala/com/intuit/data/simplan/spark/core/operators/SparkOperator.scala) class provided by Simplan framework and implements process(SparkOperatorRequest) method. The process method takes a `SparkOperatorRequest` as input and returns a `SparkOperatorResponse`. The Operator accepts a `SparkAppContext` as a constructor arguement which contains all the context information required to execute the operator.

``` scala
package com.intuit.data.simplan.spark.core.operators.transformations

imports ...

class FilteringOperator(appContext: SparkAppContext) extends SparkOperator(appContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val config: WhereConditionOperatorConfig = request.parseConfigAs[WhereConditionOperatorConfig]
    val sourceDataframe: DataFrame = request.dataframes(config.source)
    val filteredDataframe = sourceDataframe.where(config.condition)
    SparkOperatorResponse(request.taskName, filteredDataframe)
  }
}
```
`SparkOperatorRequest` contains information of all the dataframes that are available in the context. The operator can use the dataframes to perform the operation. The operator can also use the config to perform the operation. The operator can return a SparkOperatorResponse which contains the name of the task and the dataframe that is generated as a result of the operation.

## Operator Configuration
The above operator can be configured in the task as shown below:

``` hocon
TaskName {
  action {
    operator = com.intuit.data.simplan.spark.core.operators.transformations.FilteringOperator
    config = {
        source = source_table
        condition = condition
    }
  }
}
```