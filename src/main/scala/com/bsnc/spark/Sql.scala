package com.bsnc.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Sql {
  def SparkSql(msg: String) {
    val sqlContext = SparkSession
      .builder()
      .master("local[2]")
      .appName("Streaming-Consumer")
      .getOrCreate()

    import sqlContext.implicits._
    val dataSet = sqlContext.createDataset("""msg"""::Nil)
    val data = sqlContext.read.json(dataSet)
    data.show()
    //val rddDataSet = sqlContext.sparkContext.makeRDD(msg :: Nil)
    /*val dataFrame = sqlContext.read.json(Seq(msg).toDS)*/

    //data.show()
  }
}