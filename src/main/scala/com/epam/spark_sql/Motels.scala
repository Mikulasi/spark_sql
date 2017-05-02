package com.epam.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{sum, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Motels {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {

    def main(args: Array[String]): Unit = {
      require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

      val bidsPath = args(0)
      val motelsPath = args(1)
      val exchangeRatesPath = args(2)
      val outputBasePath = args(3)

      val conf = new SparkConf().setAppName("motels-home-recommendation").setMaster("local[4]")
      val sqlContext = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate()

      processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

      sqlContext.stop()
    }

    def processData(sqlContext: SparkSession, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

      /**
        * Task 1:
        * Read the bid data from the provided file.
        */
      val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

      /**
        * Task 1:
        * Collect the errors and save the result.
        */
      val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
      erroneousRecords.write
        .format(Constants.CSV_FORMAT)
        .save(s"$outputBasePath/$ERRONEOUS_DIR")

      /**
        * Task 2:
        * Read the exchange rate information.
        * Hint: You will need a mapping between a date/time and rate
        */
      val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

      /**
        * Task 3:
        * UserDefinedFunction to convert between date formats.
        * Hint: Check the formats defined in Constants class
        */
      val convertDate: UserDefinedFunction = getConvertDate

      /**
        * Task 3:
        * Transform the rawBids
        * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
        * - Convert dates to proper format - use formats in Constants util class
        * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
        */
      val bids: DataFrame = getBids(rawBids, exchangeRates)

      /**
        * Task 4:
        * Load motels data.
        * Hint: You will need the motels name for enrichment and you will use the id for join
        */
      val motels: DataFrame = getMotels(sqlContext, motelsPath)

      /**
        * Task5:
        * Join the bids with motel names.
        */
      val enriched: DataFrame = getEnriched(bids, motels)
      enriched.write
        .format(Constants.CSV_FORMAT)
        .save(s"$outputBasePath/$AGGREGATED_DIR")
    }

    def getRawBids(sqlContext: SparkSession, bidsPath: String): DataFrame = {
      import sqlContext.implicits._
      sqlContext.read.textFile(bidsPath)
        .map(m => m.split(",")).toDF()
    }

    def getErroneousRecords(rawBids: DataFrame): DataFrame = ??? /*{
//      val filter = rawBids.filter($"date" => records(2).contains("ERROR"))
//      val errRecords = filter.map(err => ("%s,%s".format(err(1), err(2)), 1)).reduceByKey((a, b) => a + b)
//      errRecords.map(pairs => "%s,%d".format(pairs._1, pairs._2))
//      val errors = rawBids.agg(
//        functions.sum(functions.when(rawBids("HU").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("HU").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("HU").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("UK").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("NL").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("US").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("MX").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("AU").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("CA").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("CN").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("KR").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("BE").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("I").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("JP").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("IN").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("HN").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("GY").isNull, 1).otherwise(0)),
//        functions.sum(functions.when(rawBids("DE").isNull, 1).otherwise(0)))

//    }*/

    def getExchangeRates(sqlContext: SparkSession, exchangeRatesPath: String): DataFrame = {
      import sqlContext.implicits._
      val usdEur = 1.025
      sqlContext.read.textFile(exchangeRatesPath)
      .map(m => m.split(","))
      .map(lines => (lines(0), lines(3).toDouble * usdEur)).toDF()
    }

    def getConvertDate: UserDefinedFunction = ???

    def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = ???

    def getMotels(sqlContext: SparkSession, motelsPath: String): DataFrame = ???

    def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???


  }
}
