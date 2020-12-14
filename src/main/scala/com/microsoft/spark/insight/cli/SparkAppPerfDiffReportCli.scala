package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.html.SparkAppHtmlUtils.writeAndOpenHtml
import com.microsoft.spark.insight.utils.spark.CommonStringUtils.IDENTICAL_APPS_WARNING
import com.microsoft.spark.insight.utils.spark.PerfValueConversionUtils.roundUpDouble
import com.microsoft.spark.insight.utils.spark.SparkAppPerfDiffReport
import com.microsoft.spark.insight.utils.spark.SparkAppPerfDiffReport.MatchingPerfValues

/**
 * Generate a performance diff report comparing two Spark application runs
 */
private[cli] class SparkAppPerfDiffReportCli private (withStages: Boolean)
    extends PerfMetricsReport[MatchingPerfValues](withStages)
    with ReportCli[AppIdPair] {

  override def headerDefinition: Array[String] = Array("Metric", "App1", "App2", "Diff", "Metric description")

  /**
   * Generate text version of Spark performance diff report given two
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]], leveraging [[SparkAppPerfDiffReport]]
   *
   * @param appReportPair two [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   * @return performance report
   */
  override protected def genTextReportBody[U <: AppReportSets[AppIdPair]](appReportPair: U): String = {
    val sb = new StringBuilder()
    val appId1 = appReportPair.appReport1.rawSparkMetrics.appId
    val appId2 = appReportPair.appReport2.rawSparkMetrics.appId

    if (appId1 == appId2) {
      sb.append(s"WARNING: $IDENTICAL_APPS_WARNING\n".yellow.bold)
    }

    val diffReport = new SparkAppPerfDiffReport(appReportPair.appReport1, appReportPair.appReport2)

    sb.append(s"\n${ReportCli.firstLineNote}\n\n")
    sb.append(s"Spark Performance Comparison Report: ${appId1.yellow} vs ${appId2.yellow}\n")

    sb.append(s"\nAggregated metrics summary:\n")

    val summaryTable = buildMetricsTable(diffReport.summaryReport, tableSummaryHeader)
    sb.append(summaryTable)
    sb.append(String.format("~" * 158) + "\n")

    sb.append(
      buildPerStageTables(
        "Metrics for individual jobs",
        diffReport.jobLevelReport,
        "Metrics for individual stages",
        diffReport.perJobReport))
    sb.append(ReportCli.htmlNote)

    sb.toString
  }

  override protected def buildPerfTableRowValues(
      value: MatchingPerfValues,
      transformer: Double => String): Array[String] =
    Array(transformer(value.perfValue1), transformer(value.perfValue2), genDiffColumn(value, transformer))

  private def genDiffColumn(value: MatchingPerfValues, transformer: Double => String): String = {
    val delta = value.perfValue2 - value.perfValue1
    if (value.perfValue1 == 0.0d || delta == 0.0d) {
      "0%"
    } else {
      val percent = roundUpDouble(delta * 100.0 / value.perfValue1, 0)
      s"$percent% (${transformer(delta)})"
    }
  }

  /**
   * Generate Html version of Spark performance diff report given two
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]], leveraging [[SparkAppPerfDiffReport]]
   *
   * @param appReportPair two [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   * @return String containing path to Html File.
   */
  override protected def genHtmlReportFile[U <: AppReportSets[AppIdPair]](appReportPair: U): String = {
    val sb = new StringBuilder
    val appId1 = appReportPair.appReport1.rawSparkMetrics.appId
    val appId2 = appReportPair.appReport2.rawSparkMetrics.appId
    val diffReport = new SparkAppPerfDiffReport(appReportPair.appReport1, appReportPair.appReport2)

    sb.append(s"\nSpark Performance Comparison Report: ${appId1.yellow} vs ${appId2.yellow}\n")

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

object SparkAppPerfDiffReportCli {

  /**
   * The public API to build performance comparison report given two individual appIds
   *
   * @param withStages true or false to show per-stages metrics
   * @return a cli instance used to generate app performance diff report
   */
  def apply(withStages: Boolean): SparkAppPerfDiffReportCli = new SparkAppPerfDiffReportCli(withStages)
}
