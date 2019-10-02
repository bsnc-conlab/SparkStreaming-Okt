package com.bsnc.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging

object StreamingLog extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}