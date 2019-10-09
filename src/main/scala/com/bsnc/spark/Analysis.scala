package com.bsnc.spark

import scala.collection.mutable.ListBuffer

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

object Analysis {
  def wordAnalysis(msg: String) = {
    val normalized: CharSequence = TwitterKoreanProcessor.normalize(msg)
    val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
    val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)
    
    val messagesList = new ListBuffer[String]()
    for (i <- 0 until stemmed.length) {
      messagesList += stemmed(i).text
    }
    
    val words = messagesList.flatMap(_.split(" "))
    val pairs = words.groupBy((word: String) => word)
    val wordCounts = pairs.mapValues(_.size)
    val keyBuf = wordCounts.keySet.toBuffer

    val strJson = new StringBuilder("{\"documentID\":\"" + "1" + "\", \"from\": \"Justin\", \"To\": \"Cybang\", \"Time\": \"2019-09-30 오전 6:41:28\", \"Text\": [")
    for (i <- 0 until wordCounts.size) {
      if (i == keyBuf.length - 1) {
        strJson.append("{\"word\": \"" + keyBuf(i) + "\", \"coutns\": \"" + wordCounts.get(keyBuf(i)).getOrElse(0) + "\"}")
      } else {
        strJson.append("{\"word\": \"" + keyBuf(i) + "\", \"coutns\": \"" + wordCounts.get(keyBuf(i)).getOrElse(0) + "\"},")
      }
    }
    strJson.append("]}")
    if(StreamingProducer.producer(strJson.toString()) == true) println("produced succeed")
    else println("produced failed")
  }
}