package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/**
 * A container for a set of jobs, indexed by their [[JobKey]] and coupled with their corresponding [[PerStageReport]]s
 * that contains a set of metrics per-stage. This is a private constructor. Callers should use the apply() method for
 * instantiating.
 *
 * @param exception an exception, in case one was thrown during construction of the input
 * @param jobs a sequence of jobs and their stages, coupled with a per-stage [[PerfMetricsType]]
 * @tparam JobKey the unique identifier to be used for jobs
 * @tparam StageKey the unique identifier to be used for stages
 * @tparam PerfMetricsType the type of [[PerfMetrics]] object to be used, per-stage
 */
class PerJobReport[JobKey, StageKey, PerfMetricsType <: PerfMetrics[Any]] private (
    exception: Option[Throwable],
    jobs: Seq[(JobKey, PerStageReport[StageKey, PerfMetricsType])])
    extends Iterable[(JobKey, PerStageReport[StageKey, PerfMetricsType])] {
  private val _failure = exception

  // Use a ListMap to preserve ordering of entries
  private val _jobMap: Map[JobKey, PerStageReport[StageKey, PerfMetricsType]] = ListMap(jobs: _*)

  /**
   * Retrieve the exception that occurred during construction, if any
   *
   * @return the exception or null if there was not failure
   */
  def failure: Throwable = _failure.orNull

  /**
   * Check whether the report was generated without failures
   *
   * @return 'true' if there were no failures, else 'false'
   */
  def isValid: Boolean = _failure.isEmpty

  /**
   * Retrieve the [[PerStageReport]] for a particular [[JobKey]]
   *
   * @param jobKey jobKey to retrieve the report for
   * @return the PerStageReport object associated with this [[JobKey]]
   */
  def job(jobKey: JobKey): PerStageReport[StageKey, PerfMetricsType] = doIfValid(() => _jobMap(jobKey))

  /**
   * Retrieve all of the [[PerStageReport]]s associated with the set of jobs
   *
   * @return an iterator over the underlying [[PerStageReport]]s
   */
  def allJobs: Iterable[PerStageReport[StageKey, PerfMetricsType]] = doIfValid(() => _jobMap.values)

  /**
   * Retrieve all of the [[JobKey]]s indexed in this collection
   *
   * @return an iterator over the jobKeys
   */
  def jobKeys: Iterable[JobKey] = doIfValid(() => _jobMap.keys)

  override def iterator: Iterator[(JobKey, PerStageReport[StageKey, PerfMetricsType])] =
    doIfValid(() => _jobMap.iterator)

  private def doIfValid[T](f: () => T): T = if (isValid) f.apply() else throw failure

  override def equals(other: Any): Boolean = other match {
    case that: PerJobReport[JobKey, StageKey, PerfMetricsType] =>
      _failure == that._failure && _jobMap.equals(that._jobMap)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_failure, _jobMap)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object PerJobReport {

  /**
   * Main entry point for this class
   *
   * @param jobsBuilder a lambda to construct the internal components to be collected by this object. May throw an
   *                    exception
   * @tparam JobKey the unique identifier to be used for jobs
   * @tparam StageKey the unique identifier to be used for stages
   * @tparam PerfMetricsType the type of [[PerfMetrics]] object to be used, per-stage
   * @return a PerJobReport object
   */
  def apply[JobKey, StageKey, PerfMetricsType <: PerfMetrics[Any]](
      jobsBuilder: () => Seq[(JobKey, Seq[(StageKey, PerfMetricsType)])])
      : PerJobReport[JobKey, StageKey, PerfMetricsType] = {
    Try(jobsBuilder.apply()) match {
      case Failure(exception) => new PerJobReport(Option(exception), Seq.empty)
      case Success(jobs) =>
        new PerJobReport(None, jobs.map { case (jobKey, perStageSeq) => (jobKey, new PerStageReport(perStageSeq)) })
    }
  }
}
