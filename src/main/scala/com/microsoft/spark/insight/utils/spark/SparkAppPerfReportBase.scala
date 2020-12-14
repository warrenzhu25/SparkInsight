package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._

import scala.collection.immutable.ListMap

/**
 * A template for constructing Spark Application Performance Reports
 *
 * Implementers will need to provide the builder methods to construct the internal structures that hold the performance
 * data. The super class implementation will then use the builders to construct the underlying [[PerJobReport]] and
 * [[PerStageReport]] objects and expose them through a unified interface. This template class is meant to reduce the
 * effort needed by consumers processing different types of reports.
 *
 * @tparam JobKey the unique identifier to be used for jobs
 * @tparam StageKey the unique identifier to be used for stages
 * @tparam PerfMetricsType the type of [[PerfMetrics]] object to be used, per-stage
 */
abstract class SparkAppPerfReportBase[JobKey, StageKey, PerfMetricsType <: PerfMetrics[Any]] {
  private val _perJobReport = PerJobReport(buildPerJobData)

  private val _summaryReport = buildSummaryReport.apply()

  private val _jobLevelReport = buildJobLevelReport.apply()

  /**
   * A lambda that produces the internal data needed to construct a [[PerJobReport]]. Namely, a sequence of pairs of
   * jobKeys, coupled with a sequence of stages per-JobKey that includes the underlying stageKeys and their matching
   * PerfMetrics. The lambda may fail without failing the construction of the entire report.
   *
   * @return a sequence of pairs of jobKeys and a sequence of pairs of stageKeys and their corresponding PerfMetrics
   */
  protected def buildPerJobData: () => Seq[(JobKey, Seq[(StageKey, PerfMetricsType)])]

  /**
   * A lambda that produces a [[PerfMetricsType]] object enclosing a summarized view of the metrics from all stages
   *
   * @return a PerfMetrics object
   */
  protected def buildSummaryReport: () => PerfMetricsType

  /**
   * A lambda that produces a Map of metrics aggregated at a job-level with
   * [[JobKey]] as keys and corresponding [[PerfMetricsType]] as values
   *
   * @return ListMap of job-level metrics
   */
  protected def buildJobLevelReport: () => ListMap[JobKey, PerfMetricsType]

  /**
   * Returns the [[PerJobReport]] object for this report, containing an internal break-down into jobs, and then stages,
   * with a per-stage [[PerfMetricsType]] object
   *
   * @return a PerJobReport object
   */
  def perJobReport: PerJobReport[JobKey, StageKey, PerfMetricsType] = _perJobReport

  /**
   * Returns the [[PerfMetricsType]] object containing summarized metrics of all of the underlying jobs and stages
   *
   * @return a PerfMetrics object
   */
  def summaryReport: PerfMetricsType = _summaryReport

  /**
   * Returns a Map of job-level metrics with [[JobKey]] as keys and corresponding [[PerfMetricsType]] as values
   *
   * @return ListMap of job-level metrics
   */
  def jobLevelReport: ListMap[JobKey, PerfMetricsType] = _jobLevelReport
}
