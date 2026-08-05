# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Overview
SimPlan for Apache Spark is an implementation of the SimPlan framework for the Spark execution engine. It provides a declarative, configuration-driven approach to building Spark data pipelines through operators. The project is built using Scala 2.12.4 with Spark 3.5.1 and uses Maven for dependency management.

This repository depends on the `simplan-framework` (located at `/Users/dyelluri1/workspace/simplan/spark/simplan-framework`) which provides the core abstractions and functionality.

## Project Structure

### Modules
- **spark-core**: The core implementation containing operators, context management, and Spark-specific functionality
- **spark-launcher**: Command-line launcher for running SimPlan Spark applications

### Key Packages (spark-core)
- `com.intuit.data.simplan.spark.core.context`: Contains `SparkAppContext` which initializes Spark sessions and manages application configuration
- `com.intuit.data.simplan.spark.core.operators`: Operator implementations organized by category:
  - `sources/`: Data sources (batch: Parquet, Avro, CSV, JSON, Delta, Kafka; streaming: Kafka, Socket, Delta)
  - `sinks/`: Data sinks (batch: Parquet, Avro, CSV, JSON, Delta, Kafka, Iceberg; streaming: Console, Delta, Kafka, Parquet, DynamoDB)
  - `transformations/`: SQL statements, joins, filters, projections, aggregations, stateful operations, windowing, DeltaMerge
  - `validators/`: Expression evaluators
  - `domainevents/`: Domain event parsing
  - `databricks/`: Databricks-specific operators (e.g., Vacuum)
- `com.intuit.data.simplan.spark.core.service`: `SparkApplication` service for running pipelines
- `com.intuit.data.simplan.spark.core.parsers`: Expression parsing and evaluation

### Configuration Files
- `spark-operator-mappings.conf`: Maps operator names to implementation classes
- `spark-config-base.conf`: Base Spark configuration including serializer settings
- Configuration uses HOCON format and Typesafe Config library

## Build Commands

### Build the project
```bash
mvn clean install
```
or use the shell script:
```bash
./build.sh
```

### Build with dependencies assembled
```bash
mvn clean install assembly:single
```
or use:
```bash
./buildwithDependencies.sh
```

### Run tests
```bash
mvn test
```

### Run tests for a specific module
```bash
mvn test -pl spark-core
```

### Run a single test class
```bash
mvn test -pl spark-core -Dtest=ClassName
```

### Run tests with coverage
```bash
mvn scoverage:report
```

### Code formatting
Format Scala code using scalafmt:
```bash
mvn scala-maven-plugin:compile
```

Configuration is in `.scalafmt.conf` with max column width of 200 and version 2.5.0.

## Architecture

### Application Entry Point
The main entry point is `SimPlanSparkLauncher` which:
1. Creates a `SparkAppContext` from command-line arguments
2. Initializes `SparkApplication` with the context
3. Runs the application with `DefaultRunParameters`

### Context Initialization
`SparkAppContext` extends `AppContext` from simplan-framework and:
- Loads configuration files (`spark-operator-mappings.conf`, `spark-config-base.conf`)
- Creates and configures SparkSession with Kryo serialization
- Optionally enables Hive support based on configuration
- Registers custom UDFs defined in configuration
- Registers qualified parameter handlers for DDL and JSON schemas
- Uses AmazonS3FileUtils for file operations

### Operator Pattern
All operators extend `SparkOperator` which provides:
- Automatic retry logic with configurable attempts and delays
- DataFrame caching/persistence with configurable storage levels
- Automatic repartitioning based on configuration
- Metrics collection (count, distinct count, count distinct by columns)
- Job group management for Spark UI tracking

Operators process `SparkOperatorRequest` and return `SparkOperatorResponse` containing named DataFrames.

### Configuration-Driven Design
Operators are instantiated dynamically based on configuration mappings. Each operator type (source, sink, transformation) has a corresponding config class that defines parameters like format, path, options, caching, repartitioning, and retry behavior.

### Testing Framework
Tests extend `SimplanSparkJobTestFunSuite` which:
- Provides a shared `SparkAppContext` across tests
- Uses ScalaTest's `AnyFunSuiteLike`
- Offers `SimplanSparkJobTestBuilder` for fluent test construction
- Automatically cleans up Spark resources and temporary files after tests
- Loads `spark-test.conf` automatically for test configurations

Test pattern:
```scala
test("test name") { builder =>
  builder
    .withConfigs("config1", "config2")
    .will { (spark, responses) =>
      // assertions
    }
}
```

## Development Guidelines

### Scala Version and Compatibility
- Use Scala 2.12.4 syntax
- Target Java 1.8 bytecode
- Follow Spark 3.5.1 API patterns

### Operator Implementation
When creating new operators:
1. Extend appropriate base class (`AbstractBatchSource`, `AbstractBatchSink`, or `SparkOperator`)
2. Implement the `process` method to return `SparkOperatorResponse`
3. Define a corresponding config class for operator parameters
4. Add mapping to `spark-operator-mappings.conf`
5. Consider caching, repartitioning, and metrics requirements using `SparkOperatorSettings`

### Configuration
- Operator options include: retry, cache, repartition, metrics
- Cache config: `persist`, `storageLevel` (MEMORY_ONLY, MEMORY_AND_DISK, etc.), `blocking`
- Retry config: `maxAttempts`, `delayInSeconds`
- Metrics config: `enabled`, `count`, `countDistinct`, `countDistinctByColumns`

### Code Style
- Maximum line length: 200 characters
- Use Scala idiomatic patterns (case classes, pattern matching, for-comprehensions)
- Prefer immutability and functional programming patterns
- Follow existing naming conventions (camelCase for methods, PascalCase for classes)

### Testing
- Place tests in `src/test/scala` with same package structure as implementation
- Extend `SimplanSparkJobTestFunSuite` for integration tests
- Use `SimplanSparkJobTestBuilder` for fluent test construction
- Clean test resources in `afterAll()` if needed

### Dependencies
The project uses Maven dependency management with version properties defined in the parent POM. Key dependencies:
- Spark Core, SQL, Kafka (3.5.1)
- Delta Lake (3.1.0)
- simplan-framework (version in property `simplan-framework.version`)
- ScalaTest for testing

When updating dependencies, modify properties in `pom.xml` rather than hardcoding versions.

## CI/CD

### Jenkins Pipeline
The project uses Jenkins for CI/CD with these key stages:
- BUILD CHECK for PRs and feature branches
- BUILDING STAGE for `main` and `develop` branches
- SonarQube analysis for code quality
- Artifact deployment to Intuit Artifactory

### Branch Strategy
- `main`: Production releases
- `develop`: Development snapshots
- Feature branches and PRs: Build verification only

### Versioning
- Main branch: Uses `config.artifactVersion` from `library-config.yaml` + build number
- Other branches: `1.0.0-{branch-name}-SNAPSHOT`
