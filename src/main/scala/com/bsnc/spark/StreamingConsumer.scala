package com.bsnc.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StreamingConsumer {
  def main(args: Array[String]) {
    StreamingLog.setStreamingLogLevels()

    val topics = Config.C_TOPIC
    val out = Config.OUT
    val kafkaParams = Config.KAFKA_CONSUMER_PARAMS
    val interval = Config.INTERVAL
    //val streamContext = Config.SPARK_STREAMING_CONTEXT
    //val sqlContext = Config.SPARK_SQL_CONTEXT
    
    val streamContext = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Streaming-Consumer")
      .set("spark.driver.allowMultipleContexts", "true")

    val ssc = new StreamingContext(streamContext, interval)
    val topicsSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      
    /*val dataFrame = sqlContext.read.json("json_sample.txt")
    dataFrame.show()*/

    val value = messages.map(_.value)
    value.saveAsTextFiles(out)

    val event = new streamListner(ssc)
    ssc.addStreamingListener(event)

    ssc.start()
    ssc.awaitTermination()
  }
}