package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.utils.spark.SparkAppPerfAggregateDiffReport.AggregateDiffPerfValue
import com.microsoft.spark.insight.utils.spark.{SparkAppPerfAggregateDiffReport, SparkAppPerfRegressionReport}

/**
 * Generate an aggregated diff performance report given two sets of Spark application runs
 */
private[cli] class SparkAppPerfAggregateDiffReportCli private (withStages: Boolean)
    extends PerfMetricsReport[AggregateDiffPerfValue](withStages)
    with ReportCli[AppIdSetPair] {

  override def headerDefinition: Array[String] =
    Array("Metric", "Min", "Max", "Median", "Mean", "Stddev", "Metric description")

  /**
   * Generate Text version of aggregated Spark performance diff report comparing two sets of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s, leveraging [[SparkAppPerfAggregateDiffReport]]
   *
   * @param appReportSetPair two sets of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return performance report
   */
  override protected def genTextReportBody[U <: AppReportSets[AppIdSetPair]](appReportSetPair: U): String = {
    val sb = new StringBuilder()
    val aggDiffReport = new SparkAppPerfAggregateDiffReport(appReportSetPair.appReports1, appReportSetPair.appReports2)

    sb.append(s"\n${ReportCli.firstLineNote}\n\n")
    sb.append(
      s"Aggregated Performance Comparison Report for two sets of Spark applications:\n${appReportSetPair.toString}\n")

    sb.append(s"\nAggregated metrics summary:\n")

    val summaryTable = buildMetricsTable(aggDiffReport.summaryReport, tableSummaryHeader)
    sb.append(summaryTable)
    sb.append(String.format("~" * 193) + "\n")

    sb.append(
      buildPerStageTables(
        "Metrics for individual jobs",
        aggDiffReport.jobLevelReport,
        "Metrics for individual stages",
        aggDiffReport.perJobReport))
    sb.append(ReportCli.htmlNote)

    sb.toString
  }

  override protected def buildPerfTableRowValues(
      value: AggregateDiffPerfValue,
      transformer: Double => String): Array[String] =
    Array(
      s"${transformer(value.aggregatePerfValue1.min)} vs ${transformer(value.aggregatePerfValue2.min)}",
      s"${transformer(value.aggregatePerfValue1.max)} vs ${transformer(value.aggregatePerfValue2.max)}",
      s"${transformer(value.aggregatePerfValue1.percentile(50))} vs " +
        s"${transformer(value.aggregatePerfValue2.percentile(50))}",
      s"${transformer(value.aggregatePerfValue1.mean)} vs ${transformer(value.aggregatePerfValue2.mean)}",
      s"${transformer(value.aggregatePerfValue1.stddev)} vs ${transformer(value.aggregatePerfValue2.stddev)}")

  /**
   * Generate Html version of aggregated Spark performance diff report comparing two sets of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s, leveraging [[SparkAppPerfAggregateDiffReport]]
   *
   * @param appReportSetPair a collection of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return String containing path to Html File.
   */
  override protected def genHtmlReportFile[U <: AppReportSets[AppIdSetPair]](appReportSetPair: U): String = {
    val sb = new StringBuilder()
    val aggDiffReport = new SparkAppPerfAggregateDiffReport(appReportSetPair.appReports1, appReportSetPair.appReports2)

    sb.append(
      s"\nAggregated Performance Comparison and statistically-based performance regression report " +
        s"for two sets of Spark applications:\n${appReportSetPair.toString}\n")

    // writeAndOpenHtml creates a new views.html file named 'appId',
    // writes the rendered views.html output, opens file in browser and returns path to file
    try {
      val regressionReport =
        new SparkAppPerfRegressionReport(appReportSetPair.appReports1, appReportSetPair.appReports2)
      val htmlFilePath = "empty"
      sb.append(s"\nHTML report was written to: $htmlFilePath".blue.bold)
    } catch {
      case e: Exception => sb.append(s"\nGenerating Html report failed. Reason: $e")
    }

    sb.toString
  }
}

object SparkAppPerfAggregateDiffReportCli {

  /**
   * The public API to build aggregated app performance report given two sets of Spark applications
   *
   * @param withStages true or false to show per-stages metrics
   * @return a cli instance used to generate aggregated app performance diff report
   */
  def apply(withStages: Boolean): SparkAppPerfAggregateDiffReportCli =
    new SparkAppPerfAggregateDiffReportCli(withStages)
}
