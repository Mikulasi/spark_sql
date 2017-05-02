package com.epam.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object bids {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("motels-home-recommendation").setMaster("local[4]")
    val sqlContext = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate()
    import sqlContext.implicits._
    val text = sqlContext.read.textFile("./resources/bids.txt")
    val row = text
      .map(_.split(",").map(_.trim))
//      .filter(f => f.size <= 3)
//      .map(err => ErrorItems(err.head, err(1), err(2)))
//      .filter(fi => fi.error.contains("ERROR") || fi.date.contains("ERROR"))
      .map(attr => BidItem(attr(0), attr(1), convertString(attr(2)), convertString(attr(3)), convertString(attr(4)), convertString(attr(5)),
        convertString(attr(6)), convertString(attr(7)), convertString(attr(8)), convertString(attr(9)), convertString(attr(10)), convertString(attr(11)),
        convertString(attr(12)), convertString(attr(13)), convertString(attr(14)), convertString(attr(15)), convertString(attr(16)), convertString(attr(17)))).toDF()
    row.createOrReplaceTempView("bids")
    val recs = sqlContext.sql("select * from bids").describe()

//      write.csv("./resources/bids.csv")

  }

  def convertString(value: String): Double = {
    try {
      value.toDouble
    } catch {
      case _: NumberFormatException => Double.NaN
    }
  }

  def getSchema: StructType = {
    StructType(
      Array(
        StructField("motelId", StringType), StructField("bidDate", StringType, nullable = true), StructField("HU", DoubleType, nullable = true),
        StructField("UK", DoubleType, nullable = true), StructField("NL", DoubleType, nullable = true), StructField("US", DoubleType, nullable = true),
        StructField("MX", DoubleType, nullable = true), StructField("AU", DoubleType, nullable = true), StructField("CA", DoubleType, nullable = true),
        StructField("CN", DoubleType, nullable = true), StructField("KR", DoubleType, nullable = true), StructField("BE", DoubleType, nullable = true),
        StructField("I", DoubleType, nullable = true), StructField("JP", DoubleType, nullable = true), StructField("IN", DoubleType, nullable = true),
        StructField("HN", DoubleType, nullable = true), StructField("GY", DoubleType, nullable = true), StructField("DE", DoubleType, nullable = true)))
  }
}
