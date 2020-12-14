package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.html.SparkAppHtmlUtils._
import com.microsoft.spark.insight.utils.PerfValue
import com.microsoft.spark.insight.utils.spark.CommonStringUtils.extractWarnings

/**
 * Generate a single app performance report given a Spark application run
 */
private[cli] class SparkAppPerfReportCli private (withStages: Boolean)
    extends PerfMetricsReport[PerfValue](withStages)
    with ReportCli[AppId] {

  override def headerDefinition: Array[String] = Array("Metric name", "Value", "Metric description")

  /**
   * Generate Text version of Spark performance report, leveraging
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   *
   * @param singleAppReport a single [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   * @return performance report
   */
  override protected def genTextReportBody[U <: AppReportSets[AppId]](singleAppReport: U): String = {
    val appReport = singleAppReport.appReport
    val sb = new StringBuilder()

    sb.append(s"\n${ReportCli.firstLineNote}\n\n")

    sb.append(s"Spark Application Performance Report for applicationId: ${appReport.rawSparkMetrics.appId}\n")

    extractWarnings(appReport).foreach(warning => sb.append(s"${"Warning:".red.underlined} $warning"))
    sb.append(s"\nSummarized metrics for all of the jobs:\n")

    val summaryTable = buildMetricsTable(appReport.summaryReport, tableSummaryHeader)
    sb.append(summaryTable)
    sb.append(String.format("~" * 136) + "\n")

    // Perform every stage's performance report
    sb.append(
      buildPerStageTables(
        "Metrics for individual jobs",
        appReport.jobLevelReport,
        "Metrics for individual stages",
        appReport.perJobReport))
    sb.append(ReportCli.htmlNote)

    sb.toString
  }

  override protected def buildPerfTableRowValues(value: PerfValue, transformer: Double => String): Array[String] =
    Array(transformer(value))

  /**
   * Generate Html version of Spark performance report, leveraging
   * [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   *
   * @param singleAppReport a single [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]
   * @return String containing path to Html File.
   */
  override protected def genHtmlReportFile[U <: AppReportSets[AppId]](singleAppReport: U): String = {
    val appReport = singleAppReport.appReport
    val sb = new StringBuilder()

    sb.append(s"\nSpark Application Performance Report for applicationId: ${appReport.rawSparkMetrics.appId}\n")

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

object SparkAppPerfReportCli {

  /**
   * The public API to build single app performance report given one appId
   *
   * @param withStages true or false to show per-stages metrics
   * @return a cli instance used to generate app performance report
   */
  def apply(withStages: Boolean): SparkAppPerfReportCli = new SparkAppPerfReportCli(withStages)
}
