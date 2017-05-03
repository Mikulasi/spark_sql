package com.epam.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.bround
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.spark_project.jetty.http.HttpStatus.Code

object bids {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("motels-home-recommendation").setMaster("local[4]")
    val sqlContext = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate()
    import sqlContext.implicits._
    //    val scv = sqlContext.read
    //      .option("delimiter", Constants.DELIMITER)
    //      .option("mode", "PERMISSIVE")
    //      .schema(getSchema)
    //      .format(Constants.CSV_FORMAT)
    //      .load("./resources/bids.txt")
    //    scv.createOrReplaceTempView("bids")
    //    val recs = sqlContext.sql("select * from bids").toDF()
    //    val err = recs.select($"MotelID", $"date", $"HU").where("HU", functions.when($"HU".like("%ERROR%")))
    //    val q = sqlContext.sql("select motelId, bidDate, HU, count(*) from bids where HU like \"ERROR\"").toDF().show()
    //    val q = recs.select("motelId","bidDate","HU").filter($"HU".rlike("ERROR")).groupBy("bidDate", "HU").agg(functions.count($"HU")).show()
    //      .write
    //      .format(Constants.CSV_FORMAT)
    //      .option("header", "true")
    //      .save("./resources/bids.csv")
    val file = sqlContext.read
      .option("delimiter", Constants.DELIMITER)
      .option("mode", "PERMISSIVE")
      .schema(Constants.EXCHANGE_RATES_HEADER)
      .format(Constants.CSV_FORMAT)
      .load("./resources/exchange_rate.txt")
    val usdEur = 1.025
    file.createOrReplaceTempView("currency")
    val q = file.select("*").groupBy("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate").agg(bround($"ExchangeRate" * usdEur, 3).as("usd_convertion")).show()
    //    sqlContext.read.textFile("./resources/exchange_rate.txt")
    //      .map(m => m.split(","))
    //      .map(lines => (lines(0), lines(3).toDouble * usdEur)).toDF().show()

  }

  def getSchema: StructType = {
    StructType(
      Array(
        StructField("motelId", StringType), StructField("bidDate", StringType, nullable = true), StructField("HU", StringType, nullable = true),
        StructField("UK", DoubleType, nullable = true), StructField("NL", DoubleType, nullable = true), StructField("US", DoubleType, nullable = true),
        StructField("MX", DoubleType, nullable = true), StructField("AU", DoubleType, nullable = true), StructField("CA", DoubleType, nullable = true),
        StructField("CN", DoubleType, nullable = true), StructField("KR", DoubleType, nullable = true), StructField("BE", DoubleType, nullable = true),
        StructField("I", DoubleType, nullable = true), StructField("JP", DoubleType, nullable = true), StructField("IN", DoubleType, nullable = true),
        StructField("HN", DoubleType, nullable = true), StructField("GY", DoubleType, nullable = true), StructField("DE", DoubleType, nullable = true)))
  }
}
