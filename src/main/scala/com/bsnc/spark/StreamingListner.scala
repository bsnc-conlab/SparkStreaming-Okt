package com.bsnc.spark

import scala.io.Source

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.OutputOperationInfo
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted

class streamListner(ssc: StreamingContext) extends StreamingListener {
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = synchronized {
    val startedBatchInfo: BatchInfo = batchStarted.batchInfo
  }
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = synchronized {
    val completedBatchInfo: BatchInfo = batchCompleted.batchInfo
    val batchTime = completedBatchInfo.batchTime
    val strBatchTime = batchTime.toString()
    val len = strBatchTime.length()
    val storedTime = strBatchTime.slice(0, len - 3)
    val fileName = "SparkFile" + "/origin-" + storedTime + "/part-00000"
    println(storedTime)
    println(fileName)
    if (Source.fromFile(fileName).getLines.isEmpty) {
      println("==listening==")
    } else {
      for (msg <- Source.fromFile(fileName).getLines) {
        Sql.SparkSql(msg)
        val analyzed = Analysis.wordAnalysis(msg, storedTime)
        val result = StreamingProducer.producer(analyzed)
        if (result == true) println("produced successful")
        else println("pruduced fail")
      }
    }
  }

  override def onBatchSubmitted(batchSumitted: StreamingListenerBatchSubmitted) = synchronized {
    val submittedBatchInfo: BatchInfo = batchSumitted.batchInfo
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted) = synchronized {
    val outputOperationStartedInfo: OutputOperationInfo = outputOperationStarted.outputOperationInfo
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted) = synchronized {
    val outputOperationCompletedInfo: OutputOperationInfo = outputOperationCompleted.outputOperationInfo
  }
}