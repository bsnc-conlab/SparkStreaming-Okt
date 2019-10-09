package com.bsnc.spark

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