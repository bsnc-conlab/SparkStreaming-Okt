package com.bsnc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies

object StreamingConsumer {
  def main(args: Array[String]) {
    StreamingLog.setStreamingLogLevels()

    val streamContext = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Streaming-Consumer")
      .set("spark.driver.allowMultipleContexts", "true")

    val ssc = new StreamingContext(streamContext, Config.INTERVAL)
    val topicsSet = Config.C_TOPIC.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, Config.KAFKA_CONSUMER_PARAMS))

    val value = messages.map(_.value)
    value.foreachRDD(rdd => {
      rdd.foreach(Analysis.wordAnalysis(_))
    })

    val event = new streamListner(ssc)
    ssc.addStreamingListener(event)

    ssc.start()
    ssc.awaitTermination()
  }
}