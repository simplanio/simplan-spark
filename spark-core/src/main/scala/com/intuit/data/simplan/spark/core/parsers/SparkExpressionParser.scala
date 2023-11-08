package com.intuit.data.simplan.spark.core.parsers

import com.intuit.data.simplan.parser.combinators.{BaseCombinatorParser, ParserUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** @author Abraham, Thomas - tabraham1
  *         Created on 19-Oct-2021 at 6:01 PM
  */
//noinspection DuplicatedCode
class SparkExpressionParser(data: Map[String, DataFrame]) extends BaseCombinatorParser {

  override def booleanCheck: Parser[Boolean] = super.booleanCheck | sparkNumericOperationComparisons | sparkOperationToNumberComparison

  //override def factor: Parser[Double] = super.factor | numericOperations

  override def numericOperations: Parser[Double] = super.numericOperations | singleArgNumericFunctions | noArgsNumericFunctions

  def sparkNumericOperations: Parser[Double] = singleArgNumericFunctions | noArgsNumericFunctions

  def sparkOperationToNumberComparison: Parser[Boolean] =
    numericOperations ~ comparator ~ expr ^^ {
      case op1 ~ comp ~ op2 => ParserUtils.numberComparisons(op1, comp, op2)
    }

  def sparkNumericOperationComparisons: Parser[Boolean] =
    numericOperations ~ comparator ~ numericOperations ^^ {
      case op1 ~ comp ~ op2 => ParserUtils.numberComparisons(op1, comp, op2)
    }

  def singleArgNumericFunctions: Parser[Double] =
    functionName ~ "(" ~ stringVar ~ "," ~ stringVar ~ ")" ^^ {
      case "sum" ~ _ ~ dataframeName ~ _ ~ column ~ _           => data(dataframeName).agg(sum(column)).first().get(0).toString.toDouble
      case "min" ~ _ ~ dataframeName ~ _ ~ column ~ _           => data(dataframeName).agg(min(column)).first().get(0).toString.toDouble
      case "max" ~ _ ~ dataframeName ~ _ ~ column ~ _           => data(dataframeName).agg(max(column)).first().get(0).toString.toDouble
      case "average" ~ _ ~ dataframeName ~ _ ~ column ~ _       => data(dataframeName).agg(avg(column)).first().get(0).toString.toDouble
      case "count" ~ _ ~ dataframeName ~ _ ~ column ~ _         => data(dataframeName).select(column).count().toDouble
      case "countDistinct" ~ _ ~ dataframeName ~ _ ~ column ~ _ => data(dataframeName).select(column).distinct().count().toDouble
    }

  def noArgsNumericFunctions: Parser[Double] =
    functionName ~ "(" ~ stringVar ~ ")" ^^ {
      case "count" ~ _ ~ dataframeName ~ _ => data(dataframeName).count().toDouble
    }
}
