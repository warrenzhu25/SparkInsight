package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.html.SparkAppHtmlUtils.writeAndOpenHtml
import com.microsoft.spark.insight.utils.RegressionAnalysis
import com.microsoft.spark.insight.utils.spark.CommonStringUtils.GRIDBENCH_LINK
import com.microsoft.spark.insight.utils.spark.PerfValueConversionUtils.roundUpDouble
import com.microsoft.spark.insight.utils.spark.{SparkAppPerfAggregateDiffReport, SparkAppPerfRegressionReport}

/**
 * Generate a performance regression report given two sets of Spark application runs
 */
private[cli] class SparkAppPerfRegressionReportCli private (withStages: Boolean)
    extends PerfMetricsReport[RegressionAnalysis](withStages)
    with ReportCli[AppIdSetPair] {

  override def headerDefinition: Array[String] =
    Array("Metric", "Effect size", "Confidence level", "Regression result", "Change percent", "Metric description")

  /**
   * Generate Text version of Spark performance regression report given two sets of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s (e.g., "before" and "after"), leveraging
   * [[SparkAppPerfRegressionReport]]
   *
   * @param appReportSetPair two sets of Spark [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return performance regression report
   */
  override protected def genTextReportBody[U <: AppReportSets[AppIdSetPair]](appReportSetPair: U): String = {
    val sb = new StringBuilder()
    val regressionReport = new SparkAppPerfRegressionReport(appReportSetPair.appReports1, appReportSetPair.appReports2)

    sb.append(s"\n${ReportCli.firstLineNote}\n\n")
    sb.append(
      s"Statistically-based performance regression report for two sets of Spark applications:\n" +
        s"${appReportSetPair.toString}\n")

    sb.append(s"\nSummarized view:\n")

    val summaryTable = buildMetricsTable(regressionReport.summaryReport, tableSummaryHeader)
    sb.append(summaryTable)

    sb.append("Detailed regression metrics explanations can be found at " + s"$GRIDBENCH_LINK/cli".blue.bold + "\n")

    sb.append(String.format("~" * 193) + "\n")

    sb.append(
      buildPerStageTables(
        "Per-job regression analysis",
        regressionReport.jobLevelReport,
        "Per-stage regression analysis",
        regressionReport.perJobReport))
    sb.append(ReportCli.htmlNote)

    sb.toString
  }

  override protected def buildPerfTableRowValues(
      value: RegressionAnalysis,
      transformer: Double => String): Array[String] = value.changePercent match {
    case num if num.isNaN => Array("N/A", "N/A", "N/A", "N/A")
    case change: Double =>
      Array(
        value.effectSize.toString,
        value.confidenceLevel.toString,
        value.regressionResult.toString,
        roundUpDouble(change, 2) + "%")
  }

  /**
   * Generate Html version of Spark performance regression report given two sets of
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s (e.g., "before" and "after"), leveraging
   * [[SparkAppPerfRegressionReport]]
   *
   * @param appReportSetPair a collection of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return String containing path to Html File.
   */
  override protected def genHtmlReportFile[U <: AppReportSets[AppIdSetPair]](appReportSetPair: U): String = {
    val sb = new StringBuilder()
    val regressionReport = new SparkAppPerfRegressionReport(appReportSetPair.appReports1, appReportSetPair.appReports2)

    sb.append(
      s"\nAggregated Performance Comparison and statistically-based performance regression report " +
        s"for two sets of Spark applications:\n ${appReportSetPair.toString}\n")

    // writeAndOpenHtml creates a new views.html file named 'appId',
    // writes the rendered views.html output, opens file in browser and returns path to file
    try {
      val aggDiffReport =
        new SparkAppPerfAggregateDiffReport(appReportSetPair.appReports1, appReportSetPair.appReports2)
      val htmlFilePath = "empty"
      sb.append(s"\nHTML report was written to: $htmlFilePath".blue.bold)
    } catch {
      case e: Exception => sb.append(s"\nGenerating Html report failed. Reason: $e")
    }

    sb.toString
  }
}

object SparkAppPerfRegressionReportCli {

  /**
   * The public API to build performance regression report given two sets of Spark applications
   *
   * @param withStages true or false to show per-stages metrics
   * @return a cli instance used to generate app performance regression report
   */
  def apply(withStages: Boolean): SparkAppPerfRegressionReportCli =
    new SparkAppPerfRegressionReportCli(withStages)
}
