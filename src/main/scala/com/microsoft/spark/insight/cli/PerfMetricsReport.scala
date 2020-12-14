package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.cli.PerfMetricsReport._
import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.utils.PerfMetrics
import com.microsoft.spark.insight.utils.spark.PerJobReport
import com.microsoft.spark.insight.utils.spark.PerfValueConversionUtils.getUnitsAndTransformer

import scala.collection.immutable.ListMap

/**
 * Base class for building PerfMetrics related tables
 *
 * @tparam T the type of PerfMetric type (e.g., PerfValue)
 * @param withStages true or false to show per-stages metrics
 */
private[cli] abstract class PerfMetricsReport[T](withStages: Boolean) {

  /**
   * Construct an array of strings containing the values to be displayed in each row
   *
   * @param value metric value
   * @param transformer a transformer method that the implementer must use to extract the value to display in each cell
   * @return one row of PerfMetrics table
   */
  protected def buildPerfTableRowValues(value: T, transformer: Double => String): Array[String]

  /**
   * Specify the header strings that will be used for table generation
   *
   * @return header definition
   */
  protected def headerDefinition: Array[String]

  /**
   * Build the summary table header based on available column names
   *
   * @return summary header
   */
  def tableSummaryHeader: PerfTableHeader = PerfTableHeader(headerDefinition, hasDescr = true)

  /**
   * Build the per-stage table header based on available column names
   *
   * @return per-stage header
   */
  def tableStageHeader: PerfTableHeader = PerfTableHeader(headerDefinition.dropRight(1), hasDescr = false)

  /**
   * Build a table with perfMetrics columns and the specified header
   *
   * Example output:
   * ╔══════════════════════════════╤═══════╤════════════════════════════════════════════════════════════════╗
   * ║ Metric name                  │ Value │ Metric description                                             ║
   * ╠══════════════════════════════╪═══════╪════════════════════════════════════════════════════════════════╣
   * ║ JVM GC Time                  │ 5     │ Total time JVM spent in garbage collection                     ║
   * ╟──────────────────────────────┼───────┼────────────────────────────────────────────────────────────────╢
   * ║ First Launch till Completed  │ 2     │ Duration between launching the first task and stage completion ║
   * ╚══════════════════════════════╧═══════╧════════════════════════════════════════════════════════════════╝
   *
   * @param perfMetrics metrics to be rendered into the table
   * @param perfHeader table header strings
   * @return Metric Table string
   */
  def buildMetricsTable(perfMetrics: PerfMetrics[T], perfHeader: PerfTableHeader): String = {
    // Generate table body contents sorted by the metric name
    val body = perfMetrics.toArray.sortBy(_._1.displayName).map {
      case (perfMetric, value) =>
        val (units, transformer) = getUnitsAndTransformer(perfMetric, 2)

        val row = Array(perfMetric.displayName) ++ buildPerfTableRowValues(value, transformer)
        if (perfHeader.hasDescr) row :+ s"${perfMetric.description} (in $units)" else row
    }

    // Make bold for metric name column
    val boldMetricName = body.indices.map(rowIndex => ((rowIndex, 0), Seq(TextStyling.BOLD))).toMap
    CliTable.createWithTextStyling(perfHeader.header, body, Map.empty, boldMetricName)
  }

  private def stageMatchWarning[J, S, P <: PerfMetrics[T]](perJobReport: PerJobReport[J, S, P]): Option[String] =
    if (perJobReport.isValid) {
      None
    } else {
      Some(
        s"Individual stage metrics cannot be rendered since the given apps don't match internally. Matching " +
          s"encountered the following error: ${perJobReport.failure.getMessage}\n".bold.yellow)
    }

  /**
   * Build per-stage report
   *
   * @param jobReportTitle the title of job-level report
   * @param jobLevelReport the corresponding job-level report
   * @param stageReportTitle the title of per-stage report
   * @param perJobReport the underlying [[PerJobReport]]
   * @tparam J Job key type
   * @tparam S Stage key type
   * @tparam P PerfMetrics type
   * @return per-stage tables report
   */
  protected def buildPerStageTables[J, S, P <: PerfMetrics[T]](
      jobReportTitle: String,
      jobLevelReport: ListMap[J, P],
      stageReportTitle: String,
      perJobReport: PerJobReport[J, S, P]): StringBuilder = {
    val sb = new StringBuilder()
    (stageMatchWarning(perJobReport), withStages) match {
      case (Some(warning), _) => sb.append(warning)
      case (None, false) =>
        sb.append("\nTip: You can check out per-stage reports by applying the optional parameter \"-A\"\n".yellow)
      case (None, true) =>
        sb.append(s"$jobReportTitle:\n\n")
        jobLevelReport.foreach { jobData =>
          val str = s"jobKey: ${jobData._1.toString.green.underlined}"
          val jobTableString = generateTable(str, jobData._2)
          sb.append(jobTableString)
        }
        sb.append(s"$stageReportTitle:\n\n")
        for {
          (jobKey, jobReport) <- perJobReport
          (stageKey, perfMetric) <- jobReport
        } {
          val str = s"jobKey: ${jobKey.toString.green.underlined}, stageKey: ${stageKey.toString.green.underlined}"
          val stageTableString = generateTable(str, perfMetric)
          sb.append(stageTableString)
        }
    }
    sb
  }

  /**
   * Generate stage-level and job-level table
   *
   * @param heading table heading
   * @param perfMetrics corresponding perfMetricsTye
   * @tparam P PerfMetrics type
   * @return table string
   */
  protected def generateTable[P <: PerfMetrics[T]](heading: String, perfMetrics: P): String = {
    val sb = new StringBuilder()
    sb.append(heading)
    val stageTable = buildMetricsTable(perfMetrics, tableStageHeader)
    sb.append(stageTable)
    sb.append(String.format("-" * 91) + "\n")
    sb.toString
  }
}

object PerfMetricsReport {

  /**
   * PerfTable header definition
   *
   * @param header the full head strings
   * @param hasDescr include or exclude the metrics description
   */
  case class PerfTableHeader(header: Array[String], hasDescr: Boolean)
}
