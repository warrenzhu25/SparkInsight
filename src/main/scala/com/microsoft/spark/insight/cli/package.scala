package com.microsoft.spark.insight

import com.microsoft.spark.insight.utils.PerfMetric
import com.microsoft.spark.insight.utils.spark.SparkAppPerfReport
import com.microsoft.spark.insight.utils.spark.SparkAppPerfReport.extractAppAttempt

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS

/**
 * A Convenient Utility shared across the entire gridbench-cli module
 */
package object cli {

  /**
   * This is a wrapper around [[SparkAppPerfReport]] to simplify the API interaction.
   * @param report given PerfReport
   */
  implicit class SparkAppPerfReportUtility(report: SparkAppPerfReport) {
    def getCpuDuration: Duration =
      Duration(report.summaryReport(PerfMetric.EXECUTOR_CPU_TIME), MILLISECONDS)

    def getSparkProps: Map[String, String] =
      extractAppAttempt(report.rawSparkMetrics).appEnvInfo.sparkProperties.toMap
  }
}
