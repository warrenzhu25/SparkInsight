package com.microsoft.spark.insight.utils

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

/**
 * A simplified implementation of [[AggregatePerfValue]] using [[DescriptiveStatistics]]
 *
 * @param perfValues values to aggregate
 */
class AggregatePerfValueImpl(perfValues: Seq[PerfValue]) extends AggregatePerfValue {
  private val _descriptiveStatistics = new DescriptiveStatistics(perfValues.length)
  perfValues.foreach(_descriptiveStatistics.addValue(_))

  override def percentile(p: Int): Double = p match {
    case per if 0 to 100 contains per => _descriptiveStatistics.getPercentile(per)
    case _ => throw new IllegalArgumentException("p must be between 0 and 100")
  }

  override def getPerfValues: Seq[PerfValue] = perfValues

  override def mean: Double = _descriptiveStatistics.getMean

  override def variance: Double = _descriptiveStatistics.getPopulationVariance

  override def stddev: Double = math.sqrt(variance)

  override def min: Double = _descriptiveStatistics.getMin

  override def max: Double = _descriptiveStatistics.getMax

  override def n: Int = _descriptiveStatistics.getN.toInt

  override def equals(other: Any): Boolean = other match {
    case that: AggregatePerfValueImpl =>
      _descriptiveStatistics.getValues.sameElements(that._descriptiveStatistics.getValues)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_descriptiveStatistics)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
