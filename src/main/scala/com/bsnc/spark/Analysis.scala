package com.bsnc.spark

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

object Analysis {
  def wordAnalysis(msg: String) = {
    val parsed = JSON.parseFull(msg)
    parsed match {
      case Some(e) =>
        val jsonMap = e.asInstanceOf[Map[String, String]]
        val subject = jsonMap.get("subject").getOrElse()
        val from = jsonMap.get("from").getOrElse()
        val to = jsonMap.get("to").getOrElse()
        val data: String = jsonMap.get("data").getOrElse("")
        val time = jsonMap.get("time").getOrElse()
        val now = System.currentTimeMillis()

        val normalized: CharSequence = TwitterKoreanProcessor.normalize(data)
        val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
        val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)

        val wordMap = scala.collection.mutable.Map[String, Int]()
        val messagesList = new ListBuffer[String]()

        for (i <- 0 until stemmed.length) {
          if (stemmed(i).pos.toString().equals("Noun")) {
            val cnt = wordMap.get(stemmed(i).text).getOrElse(0) + 1
            wordMap.put(stemmed(i).text, cnt)
          }
        }
        // List(한국어(Noun: 0, 3), 를(Josa: 3, 1),  (Space: 4, 1), 처리(Noun: 5, 2), 하다(Verb: 7, 2),  (Space: 9, 1), 예시(Noun: 10, 2), 이다(Adjective: 12, 3), ㅋㅋ(KoreanParticle: 15, 2),  (Space: 17, 1), #한국어(Hashtag: 18, 4))

        val strJson = new StringBuilder()

        strJson.append("[")
        for (key <- wordMap.keys) {
          //strJson.append("{\"index\": " + "{\"_id\": \"" + now + "_" + from + "_" + to + "_" + key + "\"}}")
          strJson.append("{\"subject\": \"" + subject + "\", \"from\": \"" + from + "\", \"to\": \"" + to + "\" , \"date\": \"" + time + "\" ,")
          strJson.append("\"word\": \"" + key + "\", \"counts\": " + wordMap.get(key).getOrElse(0) + "}")
          strJson.append(",")
        }
        strJson.delete(strJson.length-1, strJson.length)
        strJson.append("]")
        if (StreamingProducer.producer(strJson.toString()) == true) {
          //Sql.SparkSql(strJson.toString())
          println(strJson.toString())
          //println(time /*"produced succeed"*/ )
        } else println("produced failed")
    }
  }
}