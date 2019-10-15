package com.bsnc.spark

import scala.collection.mutable.ListBuffer

import org.json4s.JObject
import org.json4s.jackson.JsonMethods

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

object Analysis {
  def wordAnalysis(msg: String) = {
    val parsed = JsonMethods.parse(msg).asInstanceOf[JObject].values
    parsed match {
      case result :Map[String, Any] =>
        val jsonMap = result.asInstanceOf[Map[String, String]]
        val subject = jsonMap.get("subject").getOrElse()
        val from = jsonMap.get("from").getOrElse()
        val to = jsonMap.get("to").getOrElse()
        val data: String = jsonMap.get("data").getOrElse("")
        val time = jsonMap.get("time").getOrElse()
        val now = System.currentTimeMillis()
        
        val normalized: CharSequence = TwitterKoreanProcessor.normalize(data)
        //한국어를 처리하는 예시입니닼ㅋㅋㅋㅋㅋ -> 한국어를 처리하는 예시입니다 ㅋㅋ
        val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
        //한국어를 처리하는 예시입니다 ㅋㅋ -> 한국어Noun, 를Josa, 처리Noun, 하는Verb, 예시Noun, 입Adjective, 니다Eomi ㅋㅋKoreanParticle
        val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)
        //한국어를 처리하는 예시입니다 ㅋㅋ -> 한국어Noun, 를Josa, 처리Noun, 하다Verb, 예시Noun, 이다Adjective, ㅋㅋKoreanParticle
        val wordMap = scala.collection.mutable.Map[String, Int]()
        val messagesList = new ListBuffer[String]()

        for (i <- 0 until stemmed.length) {
          if (stemmed(i).pos.toString().equals("Noun")) {
            val cnt = wordMap.get(stemmed(i).text).getOrElse(0) + 1
            wordMap.put(stemmed(i).text, cnt)
          }
        }

        val strJson = new StringBuilder()
        strJson.append("[")
        for (key <- wordMap.keys) {
          strJson.append("{\"subject\": \"" + subject + "\", \"from\": \"" + from + "\", \"to\": \"" + to + "\" , \"date\": \"" + time + "\" ,")
          strJson.append("\"word\": \"" + key + "\", \"counts\": " + wordMap.get(key).getOrElse(0) + "}")
          strJson.append(",")
        }

        strJson.delete(strJson.length - 1, strJson.length)
        strJson.append("]")
        if (StreamingProducer.producer(strJson.toString()) == true)
          println(time + " : produced succeed - " + strJson.toString())
        else
          println("produced failed")
    }
  }
}