package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.html.SparkAppHtmlUtils.writeAndOpenHtml
import com.microsoft.spark.insight.utils.AggregatePerfValue
import com.microsoft.spark.insight.utils.spark.SparkAppPerfAggregateReport

/**
 * Generate an aggregated performance report given a set of Spark application runs
 */
private[cli] class SparkAppPerfAggregateReportCli private (withStages: Boolean)
    extends PerfMetricsReport[AggregatePerfValue](withStages)
    with ReportCli[AppIdSet] {

  override def headerDefinition: Array[String] =
    Array("Metric", "Min", "Max", "Median", "Mean", "Stddev", "Metric description")

  /**
   * Generate Text version of aggregated Spark performance report given a set of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]], leveraging [[SparkAppPerfAggregateReport]]
   *
   * @param appReportSet a set of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return performance report
   */
  override protected def genTextReportBody[U <: AppReportSets[AppIdSet]](appReportSet: U): String = {
    val sb = new StringBuilder()
    val aggReport = new SparkAppPerfAggregateReport(appReportSet.appReports)

    sb.append(s"\n${ReportCli.firstLineNote}\n\n")
    sb.append(s"Aggregated Performance Report for Spark applications: ${appReportSet.toString}\n")

    sb.append(s"\nAggregated metrics summary for all Spark applications:\n")

    val summaryTable = buildMetricsTable(aggReport.summaryReport, tableSummaryHeader)
    sb.append(summaryTable)
    sb.append(String.format("~" * 166) + "\n")

    sb.append(
      buildPerStageTables(
        "Metrics for individual jobs",
        aggReport.jobLevelReport,
        "Metrics for individual stages",
        aggReport.perJobReport))
    sb.append(ReportCli.htmlNote)

    sb.toString
  }

  override protected def buildPerfTableRowValues(
      value: AggregatePerfValue,
      transformer: Double => String): Array[String] =
    Array(
      transformer(value.min),
      transformer(value.max),
      transformer(value.percentile(50)),
      transformer(value.mean),
      transformer(value.stddev))

  /**
   * Generate Html version of aggregated Spark performance report given a set of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]], leveraging [[SparkAppPerfAggregateReport]]
   *
   * @param appReportSet a collection of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return String containing path to Html File.
   */
  override protected def genHtmlReportFile[U <: AppReportSets[AppIdSet]](appReportSet: U): String = {
    val sb = new StringBuilder()
    val aggReport = new SparkAppPerfAggregateReport(appReportSet.appReports)

    sb.append(s"\nAggregated Performance Report for Spark applications: ${appReportSet.toString}\n")

    // writeAndOpenHtml creates a new views.html file named 'appId',
    // writes the rendered views.html output, opens file in browser and returns path to file
    try {
      val htmlFilePath = "empty"
      sb.append(s"\nHTML report was written to: $htmlFilePath".blue.bold)
    } catch {
      case e: Exception => sb.append(s"\nGenerating Html report failed. Reason: $e")
    }
    sb.toString
  }
}

object SparkAppPerfAggregateReportCli {

  /**
   * The public API to build aggregated app performance report given a set of Spark applications
   *
   * @param withStages true or false to show per-stages metrics
   * @return a cli instance used to generate aggregated app performance report
   */
  def apply(withStages: Boolean): SparkAppPerfAggregateReportCli = new SparkAppPerfAggregateReportCli(withStages)
}
