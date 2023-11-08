# Getting Started

Lets go through the steps to get started with the Simplan Spark. This document provides instructions to setup and run the Simplan Spark Reference Implementation (RI) project. The project provides a sample implementation of Simplan Spark. The project can be used as a reference to understand how to use Simplan Spark framework. We will setup Simplan spark on a local machine and then run a simple example for both batch and streaming mode.

## Setup Instructions

### Step 1: Clone Simplan Spark RI
Clone the Simplan Spark Refererence implementation repository to a direcrory of your choice [https://github.intuit.com/Simplan/simplan-spark-ri/](https://github.intuit.com/Simplan/simplan-spark-ri/)

### Step 2: Update base path
Update the path of the workspace path in the file [simplan-spark-ri/src/main/resources/conf/busines-logic.simplan.conf](https://github.intuit.com/Simplan/simplan-spark-ri/blob/4e4558c5df21c9334715c4858c0160c95eed7f21/src/main/resources/conf/business-logic.simplan.conf#L3) to the path of the directory where you cloned the repository.

### Step 3: Open the project in IntelliJ
Open the project in IntelliJ. The project should be opened as a Maven project. If not, select the pom.xml file and select "Open as Maven project".

### Step 4: Run the project
Run the project by selecting the class [com.intuit.data.simplan.ri.spark.SimPlanSparkLauncherLocal](https://github.intuit.com/Simplan/simplan-spark-ri/blob/4e4558c5df21c9334715c4858c0160c95eed7f21/src/main/scala/com/intuit/data/simplan/ri/spark/SimPlanSparkLauncherLocal.scala#L27) and selecting `Run SimPlanSparkLauncherLocal` using the play button in Intellij.

Sample can be run in batch and streaming modes. You can choose the mode by passing the appropriate batch-io.simplan.conf or streaming-io.simplan.conf file as the argument to the SimPlanSparkLauncherLocal class.

### Step 5: Verifying the output
- For batch mode : The output will be present in `<projectbase>/src/main/resources/output/SimplanRI`
- For Streaming mode : The data has to be produced to simplan-ri-source topic and output will be produced to simplan-ri-sink topic.

