package com.microsoft.spark.insight.utils

/**
 * A base trait for exposing statistical methods on a given set of [[PerfValue]]s
 */
trait AggregatePerfValue {

  /**
   * Retrieve the value at the given percentile
   *
   * @param p percentile to retrieve, must be between 0 and 100
   * @return the value at the given percentile
   */
  def percentile(p: Int): Double

  def getPerfValues: Seq[PerfValue]

  def mean: Double

  def variance: Double

  def stddev: Double

  def min: Double

  def max: Double

  def n: Int
}
