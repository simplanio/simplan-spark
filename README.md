# SimPlan for Apache Spark

Implementation for Spark Execution Engine

Resources : [Simplan Framework](https://github.intuit.com/Simplan/simplan-framework) | [Simplan Documentation](https://github.intuit.com/pages/Simplan/simplan-framework/) | [Simplan for Flink](https://github.intuit.com/tabraham1/simplan-flink)

| Job | Status |
|-----|--------|
|Framework | [![Build Status](https://build.intuit.com/tech-ea/buildStatus/buildIcon?job=Simplan/simplan-framework/simplan-framework/master/)](https://build.intuit.com/tech-ea/job/Simplan/job/simplan-framework/job/simplan-framework/job/master/) |
| Spark | [![Build Status](https://build.intuit.com/tech-ea/buildStatus/buildIcon?job=Simplan/Simplan-Spark/Simplan-Spark/master//)](https://build.intuit.com/tech-ea/job/Simplan/job/Simplan-Spark/job/Simplan-Spark/job/master/) |

## Java Version Compatibility

This project supports both **Java 8** and **Java 17**. The bytecode targets Java 8, so the produced jar runs on both JVMs. A Maven profile (`java17`) auto-activates on JDK 9+ to handle the module system restrictions introduced in Java 9.

### Building

```bash
# Java 8
sdk use java 8.0.392-graal
mvn clean package

# Java 17 (java17 profile activates automatically)
sdk use java 17.0.9-graal
mvn clean package
```

### What are `--add-opens` flags?

Starting with Java 9, the **Java Platform Module System (JPMS)** was introduced. It encapsulates internal JDK packages so that libraries can no longer use reflection to access private fields and methods inside the JDK. Attempts to do so throw `InaccessibleObjectException`.

Apache Spark (and many other big data frameworks) relies heavily on reflection to access JDK internals for performance and serialization. The `--add-opens` JVM flag overrides module encapsulation for specific packages, restoring the pre-Java 9 behavior.

**Syntax:** `--add-opens=<module>/<package>=<target-module>`

- `<module>/<package>` -- the JDK module and package to open (e.g., `java.base/java.lang`)
- `<target-module>` -- who gets access. `ALL-UNNAMED` means all code on the classpath.

### Flags used in this project

| Flag | Why Spark needs it |
|------|-------------------|
| `--add-opens=java.base/java.lang=ALL-UNNAMED` | Reflection on core classes (`String`, `Thread`, etc.) for serialization and Spark internals |
| `--add-opens=java.base/java.lang.invoke=ALL-UNNAMED` | Access to `MethodHandle` internals used by Spark's code generation (Catalyst/Tungsten) |
| `--add-opens=java.base/java.lang.reflect=ALL-UNNAMED` | Reflective access to `Field`, `Method` objects for Spark's serialization framework |
| `--add-opens=java.base/java.io=ALL-UNNAMED` | Reflection on I/O classes for custom serialization (Kryo, Java serialization) |
| `--add-opens=java.base/java.net=ALL-UNNAMED` | Access to networking internals for Spark's RPC and shuffle service |
| `--add-opens=java.base/java.nio=ALL-UNNAMED` | Direct/off-heap memory buffer access used by Tungsten memory management |
| `--add-opens=java.base/java.util=ALL-UNNAMED` | Reflection on collection internals for serialization and Spark's internal data structures |
| `--add-opens=java.base/java.util.concurrent=ALL-UNNAMED` | Access to concurrent utilities for Spark's task scheduler and thread pool management |
| `--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED` | Atomic variable internals used by Spark's memory accounting and metrics |
| `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED` | NIO channel internals for Spark's shuffle and network I/O |
| `--add-opens=java.base/sun.nio.cs=ALL-UNNAMED` | Charset internals for string encoding/decoding in Spark SQL |
| `--add-opens=java.base/sun.security.action=ALL-UNNAMED` | Security privilege actions used during Spark's classloader operations |
| `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED` | Calendar internals for date/time handling in Spark SQL |
| `--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED` | Kerberos internals for secure HDFS/Hive access in authenticated clusters |

### Where are the flags applied?

| Context | How it's handled |
|---------|-----------------|
| **Build time** (Scala compiler) | Maven profile `java17` adds flags to `scala-maven-plugin` `<jvmArgs>` |
| **Test time** (ScalaTest) | Maven profile `java17` sets `${spark.test.jvmArgs}` used by `scalatest-maven-plugin` `<argLine>` |
| **Runtime** (spark-submit) | Must be passed via `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` |
| **Databricks** | Set in cluster `spark_conf` or job `new_cluster.spark_conf` (DBR 13.3+ includes most flags by default) |

### Databricks runtime notes

- **DBR 13.3 LTS and above** run on Java 17 and already include most `--add-opens` flags. You typically do not need to add them manually.
- **DBR 12.x and below** run on Java 8. No flags needed.
- If you see `InaccessibleObjectException` at runtime on DBR 13.3+, add the specific missing flag to `spark_conf`.
