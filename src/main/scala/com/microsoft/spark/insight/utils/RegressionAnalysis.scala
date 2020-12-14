package com.microsoft.spark.insight.utils

import com.microsoft.spark.insight.utils.RegressionAnalysis.ConfidenceLevel.ConfidenceLevel
import com.microsoft.spark.insight.utils.RegressionAnalysis.EffectSize.EffectSize
import org.apache.commons.math3.stat.descriptive.{StatisticalSummary, SummaryStatistics}
import org.apache.commons.math3.stat.inference.TestUtils

/**
 * Perform a statistically-based regression analysis on the given pair of sample sets.
 * This class assumes that LOWER IS BETTER for the given [[regressionMetric]].
 * The underlying statistical tools used for analyzing assume that each of the given sample sets form a normal
 * distribution.
 *
 * @param perfValues1 Sample set #1
 * @param perfValues2 Sample set #2
 * @param regressionMetric Name of the performance metric being analyzed
 */
class RegressionAnalysis(
    val perfValues1: Seq[PerfValue],
    val perfValues2: Seq[PerfValue],
    val regressionMetric: PerfMetric) {
  import com.microsoft.spark.insight.utils.RegressionAnalysis._
  import com.microsoft.spark.insight.utils.RegressionAnalysis.RegressionResult._

  private val _statSum1: StatisticalSummary = numericSeqToStatisticalSummary(perfValues1)
  private val _statSum2: StatisticalSummary = numericSeqToStatisticalSummary(perfValues2)

  private val _effectSize = EffectSize(_statSum1, _statSum2)
  private val _confidenceLevel = ConfidenceLevel(_effectSize)

  private val _regressionResult = RegressionResult(_statSum1, _statSum2)

  private val _changePercent = _statSum1.getMean match {
    case 0 => Double.NaN
    case _ => 100 * (_statSum2.getMean - _statSum1.getMean) / _statSum1.getMean
  }

  /**
   * Retrieve the first sample set
   *
   * @return StatisticalSummary object of sample set #1
   */
  def statSum1: StatisticalSummary = _statSum1

  /**
   * Retrieve the second sample set
   *
   * @return StatisticalSummary object of sample set #2
   */
  def statSum2: StatisticalSummary = _statSum2

  /**
   * Retrieve the [[EffectSize]] determined for the given pair of sample sets
   *
   * @return EffectSize
   */
  def effectSize: EffectSize = _effectSize

  /**
   * Retrieve the [[ConfidenceLevel]] determined for the given pair of sample sets
   *
   * @return ConfidenceLevel
   */
  def confidenceLevel: ConfidenceLevel = _confidenceLevel

  /**
   * Retrieve the [[RegressionResult]] determined for the given pair of sample sets
   *
   * @return RegressionResult
   */
  def regressionResult: RegressionResult = _regressionResult

  /**
   * Retrieve the change in the mean values between the given pair of sample sets in percentage
   *
   * @return The change in percentage (e.g. 18.377 means 18.377%)
   */
  def changePercent: Double = _changePercent

  override def toString: String =
    s"RegressionAnalysis for $regressionMetric: " +
      (_regressionResult match {
        case REGRESSION => f"A regression of ${_changePercent}%.0f%% was detected in sample set #2"
        case IMPROVEMENT => f"An improvement of ${_changePercent}%.0f%% was detected in sample set #2"
        case NO_CHANGE => "No statistically significant change was detected"
      }) + s"; Confidence level: ${_confidenceLevel}"
}

object RegressionAnalysis {
  val DEFAULT_ALPHA = 0.05

  /**
   * A conversion from a [[Numeric]] sequence to a [[StatisticalSummary]]
   *
   * @param seq numeric sequence
   * @param n implicit numeric object
   * @tparam T actual type for the Numeric objects
   * @return a StatisticalSummary built from the provided sequence
   */
  def numericSeqToStatisticalSummary[T](seq: Seq[T])(implicit n: Numeric[T]): StatisticalSummary = {
    val summaryStatistics = new SummaryStatistics()
    seq.map(n.toDouble).foreach(summaryStatistics.addValue)
    summaryStatistics
  }

  /**
   * The result of the regression analysis - whether a statistically significant regression/improvement has been
   * detected between the two given sample sets. This object assumes that LOWER values are BETTER.
   */
  object RegressionResult extends Enumeration {
    type RegressionResult = Value
    val NO_CHANGE, REGRESSION, IMPROVEMENT = Value

    private[utils] def apply(statSum1: StatisticalSummary, statSum2: StatisticalSummary): RegressionResult =
      isSignificantlyDifferent(statSum1, statSum2) match {
        case true if statSum1.getMean < statSum2.getMean => REGRESSION
        case true if statSum1.getMean > statSum2.getMean => IMPROVEMENT
        case _ => NO_CHANGE
      }
  }

  /**
   * Provides a mapping from an [[EffectSize]] to a confidence level that is more descriptive of its role in analyzing
   * two sample sets. Furthermore, the [[ConfidenceLevel]] logic may differ from the [[EffectSize]] when more
   * heuristics are introduced to better handle corner cases. The reason for separating those heuristics from
   * [[EffectSize]] is done in order to keep [[EffectSize]] as close as possible to its mathematical definition.
   */
  object ConfidenceLevel extends Enumeration {
    type ConfidenceLevel = Value
    val UNDEFINED, VERY_LOW, LOW, MEDIUM, HIGH = Value

    /**
     * Map the given [[EffectSize]] to a [[ConfidenceLevel]]
     *
     * @param effectSize to map
     * @return ConfidenceLevel
     */
    private[utils] def apply(effectSize: EffectSize): ConfidenceLevel = effectSize match {
      case EffectSize.UNDEFINED => UNDEFINED
      case EffectSize.TINY => VERY_LOW
      case EffectSize.SMALL => LOW
      case EffectSize.MEDIUM => MEDIUM
      case EffectSize.LARGE => HIGH
    }
  }

  /**
   * Effect Size to verbally describe a Cohen's D value.
   * A smaller effect size basically means a lower confidence level in the determining the significance of a
   * t-statistic. To increase the effect size, users can provide more samples for either sample sets.
   * @see [[https://en.wikipedia.org/wiki/Effect_size]] for more info
   */
  object EffectSize extends Enumeration {
    type EffectSize = Value
    val UNDEFINED, TINY, SMALL, MEDIUM, LARGE = Value

    private[utils] def apply(cohensd: Double): EffectSize = {
      // These are well-known values, taken from https://en.wikipedia.org/wiki/Effect_size#Cohen's_d
      if (cohensd <= 0.2) {
        TINY
      } else if (cohensd <= 0.5) {
        SMALL
      } else if (cohensd <= 0.8) {
        MEDIUM
      } else {
        LARGE
      }
    }

    private[utils] def apply(statSum1: StatisticalSummary, statSum2: StatisticalSummary): EffectSize = {
      try {
        EffectSize(cohensD(statSum1, statSum2))
      } catch {
        // EffectSize is undefined if the value of Cohen's d is non-computable in the case where the Pooled Variance = 0
        case _: IllegalArgumentException => UNDEFINED
      }
    }
  }

  /**
   * Compute the Pooled Variance for the two given sample sets.
   * @see [[https://en.wikipedia.org/wiki/Pooled_variance]]
   *
   * @param statSum1 Sample set #1
   * @param statSum2 Sample set #2
   * @return Pooled Variance
   */
  def pooledVariance(statSum1: StatisticalSummary, statSum2: StatisticalSummary): Double = {
    require(statSum1.getN + statSum2.getN > 2, "Sample sets are too small")

    ((statSum1.getN - 1) * statSum1.getVariance + (statSum2.getN - 1) * statSum2.getVariance) /
      (statSum1.getN + statSum2.getN - 2)
  }

  /**
   * Compute the Cohen's d coefficient for the two given sample sets.
   * Cohen's d is used here to determine the effect size of the sample sets under test.
   * @see [[EffectSize]]
   * @see [[https://en.wikipedia.org/wiki/Effect_size#Cohen's_d]]
   *
   * @param statSum1 Sample set #1
   * @param statSum2 Sample set #2
   * @return Cohen's d coefficient
   */
  def cohensD(statSum1: StatisticalSummary, statSum2: StatisticalSummary): Double =
    pooledVariance(statSum1, statSum2) match {
      case 0 => throw new IllegalArgumentException("Pooled variance is 0 - unable to compute the Cohen's d coefficient")
      case pooledVar => Math.abs((statSum1.getMean - statSum2.getMean) / Math.sqrt(pooledVar))
    }

  /**
   * Perform a non-equal variance, non-equal size Student's t-test (AKA Welch's t-test) with the given "alpha" value.
   *
   * A lower alpha effectively means a stringent statistical significance threshold - a lower alpha value will require
   * a stronger statistically significant difference to exist between the two sample sets for rejecting the t-test.
   *
   * @param statSum1    Sample set #1
   * @param statSum2    Sample set #2
   * @param alpha alpha value to use for the t-test
   * @return True iff there is a statistically significant difference between the two sample sets with the given alpha
   */
  def isSignificantlyDifferent(statSum1: StatisticalSummary, statSum2: StatisticalSummary, alpha: Double): Boolean =
    TestUtils.tTest(statSum1, statSum2, alpha)

  /**
   * Perform a non-equal variance, non-equal size Student's t-test (AKA Welch's t-test) with [[DEFAULT_ALPHA]] as the
   * "alpha" value.
   *
   * @param statSum1 Sample set #1
   * @param statSum2 Sample set #2
   * @return True iff there is a statistically significant difference between the two sample sets with the given alpha
   */
  def isSignificantlyDifferent(statSum1: StatisticalSummary, statSum2: StatisticalSummary): Boolean =
    isSignificantlyDifferent(statSum1, statSum2, DEFAULT_ALPHA)
}
