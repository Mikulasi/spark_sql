package com.epam.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

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
      val convertDate: UserDefinedFunction = convert_date

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
      val file = sqlContext.read
        .option("delimiter", Constants.DELIMITER)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .format(Constants.CSV_FORMAT)
        .load(bidsPath)
      file.createOrReplaceTempView("bids")
      sqlContext.sql("select * from bids").toDF()
    }

    def schema: StructType = {
      StructType(
        Array(
          StructField("motelId", StringType), StructField("bidDate", StringType, nullable = true), StructField("HU", StringType, nullable = true),
          StructField("UK", DoubleType, nullable = true), StructField("NL", DoubleType, nullable = true), StructField("US", DoubleType, nullable = true),
          StructField("MX", DoubleType, nullable = true), StructField("AU", DoubleType, nullable = true), StructField("CA", DoubleType, nullable = true),
          StructField("CN", DoubleType, nullable = true), StructField("KR", DoubleType, nullable = true), StructField("BE", DoubleType, nullable = true),
          StructField("I", DoubleType, nullable = true), StructField("JP", DoubleType, nullable = true), StructField("IN", DoubleType, nullable = true),
          StructField("HN", DoubleType, nullable = true), StructField("GY", DoubleType, nullable = true), StructField("DE", DoubleType, nullable = true)))
    }

    def getErroneousRecords(rawBids: DataFrame): DataFrame = {
      rawBids.select("motelId", "bidDate", "HU")
        .filter(col("HU").rlike("ERROR"))
        .groupBy("bidDate", "HU")
        .agg(count(col("HU")))
    }


    def getExchangeRates(sqlContext: SparkSession, exchangeRatesPath: String): DataFrame = {
      sqlContext.read
        .option("delimiter", Constants.DELIMITER)
        .option("mode", "PERMISSIVE")
        .schema(Constants.EXCHANGE_RATES_HEADER)
        .format(Constants.CSV_FORMAT)
        .load("./resources/exchange_rate.txt")
    }

    def convert_date: UserDefinedFunction = {
      udf((func: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(func).toString(Constants.OUTPUT_DATE_FORMAT))
    }

    def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
      val usdEur = 1.025
      //      val wq = exchangeRates.createOrReplaceTempView("currency")
      exchangeRates
        .select(convert_date(col("ValidFrom")) as "valid_from")
        .select("CurrencyName", "CurrencyCode", "ExchangeRate")
        .groupBy("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
        .agg(bround(col("ExchangeRate") * usdEur, 3).as("usd_convertion"))
      rawBids
        .filter(not(col("HU").contains("ERROR")))
        .select("motelId")
        .select(convert_date(col("bidDate")))
        .select("US", "CA", "MX")
        .groupBy("US", "CA", "MX")
      exchangeRates.join(rawBids, exchangeRates("valid_from") === rawBids("motelId")).groupBy(rawBids("US", "CA", "MX"), exchangeRates("ExchangeRate"))
    }

    def getMotels(sqlContext: SparkSession, motelsPath: String): DataFrame = ???

    def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???


  }
}
