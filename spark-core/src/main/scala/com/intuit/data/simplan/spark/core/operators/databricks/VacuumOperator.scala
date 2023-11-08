package com.intuit.data.simplan.spark.core.operators.databricks

import com.intuit.data.simplan.core.domain.Lineage
import com.intuit.data.simplan.core.domain.operator._
import com.intuit.data.simplan.core.util.RuntimeUtils.isDatabricksRuntime
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

/**
 * @author Kiran Hiremath
 */
case class VacuumOperatorConfig(isPerformVacuum: Boolean = false) extends OperatorConfig

class VacuumOperator(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[VacuumOperatorConfig](sparkAppContext, operatorContext) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(classOf[VacuumOperator])

  override implicit lazy val operatorConfig: VacuumOperatorConfig =operatorContext.parseConfigAs[VacuumOperatorConfig]

  private def parseSqlUsingSparkLogicalPlan(sqls: Option[List[String]], spark: SparkSession) = {

    val regexList = List("""InsertIntoStatement 'UnresolvedRelation \[(.+?)\]""".r,
      """ReplaceTableAsSelect .* 'UnresolvedTableName \[(.+?)\]""".r,
      """CreateTableAsSelect .* 'UnresolvedTableName \[(.+?)\]""".r,
      """CreateTable .* 'UnresolvedTableName \[(.+?)\]""".r,
      """SetTableLocation .* 'UnresolvedTable \[(.+?)\]""".r,
      """SetLocation .* 'UnresolvedTableName \[(.+?)\]""".r,
      """SetTableProperties .* 'UnresolvedTable \[(.+?)\]""".r,
      """DropColumns .* 'UnresolvedTable \[(.+?)\]""".r,
      """RenameColumn .* 'UnresolvedTable \[(.+?)\]""".r,
      """AlterColumn .* 'UnresolvedTable \[(.+?)\]""".r,
      """ReplaceColumns .* 'UnresolvedTable \[(.+?)\]""".r,
      """AddColumns .* 'UnresolvedTable \[(.+?)\]""".r,
      """ReplaceTable .* 'UnresolvedTableName \[(.+?)\]""".r,
      """RenameTable .* 'UnresolvedTableOrView \[(.+?)\]""".r)

    val createTableList = scala.collection.mutable.Set[String]()
    sqls.get.foreach(sql => {
      logger.debug("Parsing the sql: " + sql)
      try {
        val logicalPlan = spark.sessionState.sqlParser.parsePlan(sql).toString().replaceAll("\n", "")
        logger.debug("Logical plan "+ logicalPlan)
        regexList.map(regex => {
          regex.findFirstMatchIn(logicalPlan) match {
            case Some(regex(schemaNName)) =>
              if (Option(schemaNName).isDefined && schemaNName.nonEmpty && schemaNName.contains(",")) { // To ensure the schema name and name is present
                logger.debug("Schema and table name: " + schemaNName)
                createTableList.add(
                  schemaNName.split(",").map(_.trim).mkString(".")
                )
              }
            case None => // unable to find, ignore
          }
        })
      } catch {
        case ex: Exception => logger.warn(s"Unable to parse the ${sql} using logical plan")
      }
    })
    createTableList.toSet
  }

  def identifyTargetTables(knownCreatedTableList: Set[String], sqls: Set[String], skipList: Option[Set[String]] = None): Set[String] = {
    try {
      // Auto-onboard to DQM
      val regex: Regex = """(?i)^\s*drop\s+table\s+(if\s+exists\s+)?("?\w[\w\s]*\w"?\.)?("?\w[\w\s]*\w"?)\s*?;?\s*$""".r
      val dropTableList = scala.collection.mutable.Set[String]()
      val createTableList = scala.collection.mutable.Set[String]() ++ knownCreatedTableList

      sqls.foreach {
        case regex(_, schema, name) =>
          if (Option(schema).isDefined && schema.nonEmpty && Option(name).isDefined && schema.nonEmpty) { // To ensure the schema name and name is present
            dropTableList.add(schema + name)
          }
        case _ => false
      }

      // Finally prepare the auto-onboard table list to DQM
      val dropTablesRemovedList = createTableList -- dropTableList
      val skipTablesRemovedList = if (skipList.isDefined) dropTablesRemovedList -- skipList.get
      else dropTablesRemovedList

      skipTablesRemovedList.toSet
    } catch {
      case ex: Exception => logger.warn("Unable to get the list of target tables using lineage parse")
        Set.empty
    }
  }

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {

    if(isDatabricksRuntime && operatorConfig.isPerformVacuum) {
      val queries = Some(request.xCom
        .filter(_._2 operatorExtends classOf[Operator])
        .map(each => (each._2.operatorResponse.responseValues)).flatten
        .filter(key => key._1.equalsIgnoreCase(Lineage.RESPONSE_VALUE_KEY)).map(_._2.asInstanceOf[Lineage].statement).toList)

      logger.info(s"Perform Vacuum for pipelineName ${sparkAppContext.appContextConfig.application.parent} , jobName ${sparkAppContext.appContextConfig.application.name}")

      // Try parsing using Spark logical plan
      logger.info("Vacuum List of queries to parse " + queries.get.toString())

      val createTableList: Set[String] = parseSqlUsingSparkLogicalPlan(queries, sparkAppContext.spark)
      logger.info("createTableList in Logical parse " + createTableList)

      // Identify the list of target tables
      val finalTableSet = identifyTargetTables(createTableList, queries.get.toSet, None)
      logger.info("List of tables qualifying for Vacuum " + finalTableSet.toString())
      finalTableSet.foreach(table => {
          try {
            logger.info(s"Performing Vacuum on table : ${table}")
            sparkAppContext.spark.sql("VACUUM " + table)
          } catch {
            case ex: Exception => logger.warn(s"Unable to perform Vacuum operation on table : ${table}")
          }
        })
    } else {
      logger.info("Vacuum operation has been disabled or its not a DataBricks compute")
    }
    SparkOperatorResponse.continue
  }

}
