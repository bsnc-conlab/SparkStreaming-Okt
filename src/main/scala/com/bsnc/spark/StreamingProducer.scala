package com.bsnc.spark

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object StreamingProducer {
  def producer(analyzed: String) = {

    val brokers = Config.BROKERS
    val groupId = Config.GROUPID
    val topic = Config.P_TOPIC
    val configuration = Config.KAFKA_PRODUCER_PARAMS

    val producer = new KafkaProducer[String, String](configuration)
    val data = new ProducerRecord[String, String](topic, analyzed)
    
    producer.send(data)
    producer.close()

    true
  }
}