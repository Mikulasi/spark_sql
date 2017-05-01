package com.epam.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object bids {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("motels-home-recommendation").setMaster("local[4]")
    val sqlContext = new SparkSession.Builder().config(conf).enableHiveSupport().getOrCreate()

    sqlContext.read.textFile("./resources/bids.txt").limit(10).show()
  }
}
