package com.bsnc.spark

import org.apache.spark.sql.SparkSession

object Sql {
  def SparkSql(msg: String) {
    val sqlContext = SparkSession
      .builder()
      .master("local[2]")
      .appName("Streaming-Consumer")
      .getOrCreate()

    println(msg)
    val rddDataSet = sqlContext.sparkContext.makeRDD(msg :: Nil)
    val dataFrame = sqlContext.read.json(rddDataSet)

    dataFrame.show()
  }
}