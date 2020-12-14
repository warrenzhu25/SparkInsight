package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._

import scala.collection.immutable.ListMap

/**
 * A container for a set of stages, indexed by their [[StageKey]] and coupled with their corresponding per-stage
 * metrics
 *
 * @param stages a sequence of stages and their metrics
 * @tparam StageKey the unique identifier to be used for stages
 * @tparam PerfMetricsType the type of [[PerfMetrics]] object to be used, per-stage
 */
class PerStageReport[StageKey, PerfMetricsType <: PerfMetrics[Any]](stages: Seq[(StageKey, PerfMetricsType)])
    extends Iterable[(StageKey, PerfMetricsType)] {
  // Use a ListMap to preserve ordering of entries
  private val _stageMap: Map[StageKey, PerfMetricsType] = ListMap(stages: _*)

  /**
   * Retrieve the [[PerfMetricsType]] for a particular [[StageKey]]
   *
   * @param stageKey stageKey to retrieve metrics for
   * @return metrics
   */
  def stage(stageKey: StageKey): PerfMetricsType = _stageMap(stageKey)

  /**
   * Retrieve all of the [[PerfMetricsType]] objects associated with the set of stages
   *
   * @return an iterator over the underlying [[PerfMetricsType]] objects
   */
  def allStages: Iterable[PerfMetricsType] = _stageMap.values

  /**
   * Retrieve all of the [[StageKey]]s indexed in this collection
   *
   * @return an iterator over the stageKeys
   */
  def stageKeys: Iterable[StageKey] = _stageMap.keys

  override def iterator: Iterator[(StageKey, PerfMetricsType)] = _stageMap.iterator

  override def equals(other: Any): Boolean = other match {
    case that: PerStageReport[StageKey, PerfMetricsType] => _stageMap.equals(that._stageMap)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_stageMap)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
