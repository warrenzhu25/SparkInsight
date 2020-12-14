package com.microsoft.spark.insight.html

import java.awt.Desktop
import java.io.{File, PrintWriter}
import java.net.URI

import com.microsoft.spark.insight.utils.PerfMetric._
import com.microsoft.spark.insight.utils.spark.PerfValueConversionUtils._
import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.JobId
import com.microsoft.spark.insight.utils.spark.SparkAppPerfDiffReport.{MatchingPerfValues, MatchingStageIds, PerfMetricsDiff}
import com.microsoft.spark.insight.utils.spark.SparkAppPerfReport.StageIdAttemptId
import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher.MatchingJobIds
import com.microsoft.spark.insight.utils.spark.{SparkAppPerfDiffReport, SparkAppPerfReport}
import com.microsoft.spark.insight.utils.{AggregatePerfValue, PerfMetric, PerfMetrics, PerfValue}
import play.twirl.api.HtmlFormat.Appendable

import scala.io.Source

/**
 * Utility object for writing and displaying views.html files
 */
object SparkAppHtmlUtils {

  /**
   * Writes htmlContent to a file and opens it in browser
   *
   * @param htmlContent is the content to be written to views.html file
   * @param fileName specifies the file name
   * @return path to file
   */
  def writeAndOpenHtml(htmlContent: Appendable, fileName: String): String = {
    val file = new File(s"$USER_HOME_PATH/.gridbench", s"$fileName.views.html")
    writeToHtml(htmlContent, file)
    openFileInBrowser(file)
    file.toURI.toString
  }

  /**
   * Writes htmlContent to file
   *
   * @param htmlContent is the content to be writen to views.html file
   * @param file is the file to which content is to be written to
   */
  def writeToHtml(htmlContent: Appendable, file: File): Unit = {
    val writer = new PrintWriter(file)
    try {
      // remove redundant empty lines in htmlContent and write it to file
      writer.write(htmlContent.body.replaceAll("""(?m)^\s+$""", ""))
    } catch {
      case exception: Exception => throw exception
    } finally {
      writer.close()
    }
  }

  /**
   * Open a browser window to display the rendered HTML (for manual use)
   *
   * @param file - file to be opened in browser
   */
  def openFileInBrowser(file: File): Unit = {
    if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) {
      Desktop.getDesktop.browse(new URI(s"file://${file.getCanonicalPath}"))
    }
  }

  /**
   * convert resource contents to string
   *
   * @param path - path to the file
   * @return contents of file in a line separated string.
   */
  def resourceToString(path: String): String = {
    val stream = Source.getClass.getResourceAsStream(path)
    val lines = Source.fromInputStream(stream).getLines
    lines.mkString("\n")
  }

  /**
   * retrieve app history server and generate link to application's history page.
   *
   * @param sparkAppPerfReport an instance of [[SparkAppPerfReport]]
   * @return If history Server is found, returns views.html <a> tag containing link to history Server page of application
   *         else, application id.
   */
  def retrieveAppHistoryServerLink(sparkAppPerfReport: SparkAppPerfReport): String = {
    val historyServer = sparkAppPerfReport.rawSparkMetrics.firstAppAttempt.appEnvInfo.sparkProperties
      .find(_._1.equals("spark.yarn.historyServer.address"))
      .map(_._2)
    val appId = sparkAppPerfReport.rawSparkMetrics.appId
    historyServer match {
      case Some(server: String) =>
        s"""<a href= "http://$server/history/$appId/1/jobs}" target="_blank"> $appId</a>"""
      case _ => appId
    }
  }

  /**
   * Return a seq of All Jobs All Stages metrics
   *
   * @param sparkAppPerfReport an instance of [[SparkAppPerfReport]]
   * @return all stages metrics
   */
  def allJobsAllStagesMetrics(
      sparkAppPerfReport: SparkAppPerfReport): Seq[(StageIdAttemptId, Seq[(PerfMetric, PerfValue)])] =
    sparkAppPerfReport.perJobReport.allJobs
      .map(_.toSeq)
      .toSeq
      .reduce(_ ++ _)
      .map(v => (v._1, v._2.toSeq))
      .sortBy(_._1.toString)

  /**
   * For a given jobKey, return a seq of All Stages metrics
   *
   * @param sparkAppPerfReport an instance of [[SparkAppPerfReport]]
   * @param jobKey             JobId for which metricsList is to be retrieved.
   * @return job's stage metrics
   */
  def jobStageMetrics(
      sparkAppPerfReport: SparkAppPerfReport,
      jobKey: JobId): Seq[(StageIdAttemptId, Seq[(PerfMetric, PerfValue)])] =
    sparkAppPerfReport.perJobReport
      .job(jobKey)
      .toSeq
      .map(v => (v._1, v._2.toSeq))
      .sortBy(_._1.toString)

  /**
   * Return a sequence of All Jobs All Stage Metrics
   *
   * @param sparkAppPerfDiffReport an instance of [[SparkAppPerfDiffReport]]
   * @return all stage metrics
   */
  def allJobsAllStagesMetrics(
      sparkAppPerfDiffReport: SparkAppPerfDiffReport): Seq[(MatchingStageIds, Seq[(PerfMetric, MatchingPerfValues)])] =
    sparkAppPerfDiffReport.perJobReport.allJobs
      .map(_.toSeq)
      .toSeq
      .reduce(_ ++ _)
      .map(v => (v._1, v._2.toSeq))
      .sortBy(_._1.toString)

  /**
   * For a given jobKey, return a seq of All Stages metrics
   *
   * @param sparkAppPerfDiffReport an instance of [[SparkAppPerfDiffReport]]
   * @param jobKey Matching Job Id for which metricsList is to be retrieved.
   * @return matching Job's stage metrics
   */
  def jobStageMetrics(
      sparkAppPerfDiffReport: SparkAppPerfDiffReport,
      jobKey: MatchingJobIds): Seq[(MatchingStageIds, Seq[(PerfMetric, MatchingPerfValues)])] =
    sparkAppPerfDiffReport.perJobReport
      .job(jobKey)
      .toSeq
      .map(v => (v._1, v._2.toSeq))
      .sortBy(_._1.toString)

  /**
   * for a given metrics list, return a sequence of rows to be displayed in the table
   *
   * @param perfMetrics corresponding perfmetrics
   * @return a sequence table rows.
   *         Each row is a sequence of entries
   */
  def tableRows(perfMetrics: PerfMetrics[PerfValue]): Seq[Seq[String]] = {
    val body = perfMetrics.toSeq.sortBy(_._1.displayName).map {
      case (perfMetric, value) =>
        val (units, transformer) = getUnitsAndTransformer(perfMetric, PERF_VALUE_PRECISION_HTML)
        Seq(perfMetric.displayName, transformer(value), s"${perfMetric.description} (in $units)")
    }
    Seq("Metric Name", "Metric Value", "Metric Description") +: body
  }

  /**
   * generate rows for tables in by-metric-tab
   *
   * @param stageMetrics seq of stage id and corresponding perfValue
   * @param metric corresponding metric
   * @return sequence of table rows.
   *         Each row is a sequence of table entries
   */
  def tableRows(stageMetrics: Seq[(StageIdAttemptId, PerfValue)], metric: PerfMetric): Seq[Seq[String]] = {
    val (units, transformer) = getUnitsAndTransformer(metric, PERF_VALUE_PRECISION_HTML)
    val metricSum: Double = stageMetrics.map(_._2).sum.toDouble

    var body = stageMetrics.take(NO_OF_TABLE_ROWS_HTML).map {
      case (stageId, value) =>
        val percentage = if (metricSum > 0) roundUpDouble(value / metricSum * 100, PERF_VALUE_PRECISION_HTML) else "0"
        Seq(stageId.toString, transformer(value), percentage)
    }

    if (stageMetrics.length > NO_OF_TABLE_ROWS_HTML) {
      val othersSum = metricSum - stageMetrics.take(NO_OF_TABLE_ROWS_HTML).map(_._2).sum
      val othersPercentage =
        if (metricSum > 0) roundUpDouble(othersSum / metricSum * 100, PERF_VALUE_PRECISION_HTML) else "0"
      body = body :+ Seq("Others", transformer(othersSum), othersPercentage)
    }

    Seq("Stage Id", s"Metric Value (in $units)", "Percentage") +: body :+
      Seq("TOTAL", transformer(metricSum), if (metricSum > 0) "100.000" else "0.000")
  }

  /**
   * generate pie chart data.
   *
   * @param stageMetrics seq of stage id and corresponding perfValue
   * @param metric corresponding metric
   * @return Sequence of Strings corresponding to pie sectors.
   */
  def pieSectors(stageMetrics: Seq[(StageIdAttemptId, PerfValue)], metric: PerfMetric): Seq[(String, String)] = {
    val transformer = getUnitsAndTransformer(metric, PERF_VALUE_PRECISION_HTML)._2

    var sectors = stageMetrics.take(NO_OF_PIE_SECTORS).map {
      case (stageId, value) =>
        (stageId.toString(), transformer(value))
    }

    if (stageMetrics.length > NO_OF_PIE_SECTORS) {
      val othersSum: Double = stageMetrics.map(_._2).sum - stageMetrics.take(NO_OF_PIE_SECTORS).map(_._2).sum
      sectors = sectors :+ Tuple2("Others", transformer(othersSum))
    }
    sectors
  }

  /**
   * generates bar chart values in by metric tab in views.html version of diff report.
   *
   * @param stageMetrics seq of matching stage ids and corresponding perfValues.
   * @param metric corresponding metric
   * @return Sequence of tuple corresponding to bar chart entries.
   */
  def diffBarChartValues(
      stageMetrics: Seq[(MatchingStageIds, MatchingPerfValues)],
      metric: PerfMetric): Seq[(String, String, String, String)] = {
    val transformer = getUnitsAndTransformer(metric, PERF_VALUE_PRECISION_HTML)._2
    val sectors = diffByMetricRows(stageMetrics, NO_OF_PIE_SECTORS, transformer)
    sectors.take(sectors.length - 1)
  }

  /**
   * generate Table rows for by job tab in views.html version of diff report.
   *
   * Uses the sequence of tuples generated by [[diffByMetricRows()]]
   * @param perfMetricsDiff an instance of [[PerfMetricsDiff]]
   * @return sequence of table rows.
   */
  def diffByJobTableRows(perfMetricsDiff: PerfMetricsDiff): Seq[Seq[String]] = {
    val body: Seq[Seq[String]] =
      perfMetricsDiff.toSeq.sortBy(_._1.displayName).map {
        case (perfMetric, matchingPerfValues) =>
          val (units, transformer) = getUnitsAndTransformer(perfMetric, PERF_VALUE_PRECISION_HTML)
          Seq(
            perfMetric.displayName,
            transformer(matchingPerfValues.perfValue1),
            transformer(matchingPerfValues.perfValue2),
            transformer(matchingPerfValues.perfValue2 - matchingPerfValues.perfValue1),
            s"${perfMetric.description} (in $units)")
      }
    Seq("Metric Name", "App #1 Metric Value", "App #2 Metric Value", "Diff", "Description") +: body
  }

  /**
   * generate Table rows for by metric tab in views.html version of diff report.
   *
   * Uses the sequence of tuples generated by [[diffByMetricRows()]]
   * @param stageMetrics seq of matching stage ids and corresponding perfValues.
   * @param metric corresponding metric
   * @return Sequence of table rows.
   */
  def diffByMetricTableRows(
      stageMetrics: Seq[(MatchingStageIds, MatchingPerfValues)],
      metric: PerfMetric): Seq[Seq[String]] = {
    val (units, transformer) = getUnitsAndTransformer(metric, PERF_VALUE_PRECISION_HTML)
    val rows = diffByMetricRows(stageMetrics, NO_OF_TABLE_ROWS_HTML, transformer)
    val body = rows.map {
      case (stageId, perfValue1, perfValue2, diff) =>
        Seq(stageId, perfValue1, perfValue2, diff)
    }

    Seq("Stage Id", s"App #1 Metric Value (in $units)", s"App #2 Metric Value (in $units)", s"Diff (in $units)") +: body
  }

  /**
   * generate a sequence of tuples containing stage data for given stage metrics
   *
   * @param stageMetrics seq of matching stage ids and corresponding perfValues.
   * @param noOfEntries number of entries that should be returned.
   * @param transformer corresponding transformer for the metric.
   * @return a sequence of tuples
   */
  def diffByMetricRows(
      stageMetrics: Seq[(MatchingStageIds, MatchingPerfValues)],
      noOfEntries: Int,
      transformer: Double => String): Seq[(String, String, String, String)] = {
    val values = (stageMetrics.map(stage => stage._2.perfValue2 - stage._2.perfValue1) zip stageMetrics).sortBy(_._1)

    var entries = values.take(noOfEntries).map {
      case (diff, (stageId, matchingPerfValue)) =>
        (
          stageId.toString,
          transformer(matchingPerfValue.perfValue1),
          transformer(matchingPerfValue.perfValue2),
          transformer(diff))
    }

    if (values.length > noOfEntries) {
      val othersSum1 = values.map(_._2._2.perfValue1).sum - values.take(noOfEntries).map(_._2._2.perfValue1).sum
      val othersSum2 = values.map(_._2._2.perfValue2).sum - values.take(noOfEntries).map(_._2._2.perfValue2).sum
      val diffOthersSum = values.map(_._1).sum - values.take(noOfEntries).map(_._1).sum
      entries =
        entries :+ Tuple4("Others", transformer(othersSum1), transformer(othersSum2), transformer(diffOthersSum))
    }
    entries :+ Tuple4(
      "TOTAL",
      transformer(values.map(_._2._2.perfValue1).sum),
      transformer(values.map(_._2._2.perfValue2).sum),
      transformer(values.map(_._1).sum))
  }

  /**
   * For a given aggregatePerfValue, generates the entries required in the Box chart
   *
   * @param aggregatePerfValue : an instance of [[AggregatePerfValue]]
   * @param metric : corresponding metric
   * @return a sequence of String
   */
  def boxPlotValues(aggregatePerfValue: AggregatePerfValue, metric: PerfMetric): Seq[String] = {
    val transformer = getUnitsAndTransformer(metric, 4)._2
    Seq(
      transformer(aggregatePerfValue.percentile(25)),
      transformer(aggregatePerfValue.percentile(50)),
      transformer(aggregatePerfValue.percentile(75)),
      transformer(aggregatePerfValue.min),
      transformer(aggregatePerfValue.max),
      transformer(aggregatePerfValue.mean),
      transformer(aggregatePerfValue.stddev))
  }

  /**
   * For a given Job key and SparkAppPerfReport, return the job name
   *
   * @param jobKey JobId of the Job
   * @param sparkAppPerfReport an instance of [[SparkAppPerfReport]]
   * @return Job name of the corresponding job
   */
  def jobName(jobKey: JobId, sparkAppPerfReport: SparkAppPerfReport): String =
    sparkAppPerfReport.rawSparkMetrics.appAttemptMap.values.toSeq.head.jobDataMap(jobKey).name

  /**
   * get user homePath to save Html file
   */
  val USER_HOME_PATH: String = System.getProperty("user.home")

  /**
   * list of metrics to be included in pie chart
   */
  val PIE_METRICS_LIST: Seq[PerfMetric] =
    Seq(SHUFFLE_READ_FETCH_WAIT_TIME, SHUFFLE_WRITE_TIME, EXECUTOR_CPU_TIME, NET_IO_TIME, JVM_GC_TIME)

  /**
   * List of metrics available in stage report.
   */
  val STAGE_METRICS_LIST: Seq[PerfMetric] =
    MILLIS_TO_MINUTES_PERF_METRICS ++ TO_THOUSANDS_PERF_METRICS ++ BYTES_TO_GB_PERF_METRICS

  /**
   * The precision to which perfValues have to be displayed in GridBench Html
   */
  val PERF_VALUE_PRECISION_HTML = 4

  /**
   * Number of rows to be displayed in Html if limited rows are to be displayed.
   *
   * This is not used for tables showing all perfMetrics and their values (example: Summary Report)
   */
  val NO_OF_TABLE_ROWS_HTML = 10

  /**
   * Number of sectors to be displayed in Html if limited sectors are to be displayed.
   */
  val NO_OF_PIE_SECTORS = 10
}
