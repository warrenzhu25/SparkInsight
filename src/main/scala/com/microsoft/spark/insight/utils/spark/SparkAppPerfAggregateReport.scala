package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.spark.SparkAppPerfAggregateReport._
import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher._
import com.microsoft.spark.insight.utils.{AggregatePerfValue, AggregatePerfValueImpl}

import scala.collection.immutable.ListMap

/**
 * Construct an aggregated report for the given set of [[SparkAppPerfReport]]s. The report will try to match all of
 * the given app reports, and if they match, will produce an [[AggregatePerfMetrics]] per every given matching stageIds.
 * The report will also produce an [[AggregatePerfMetrics]] for the summaries of each app report
 *
 * @param appPerfReports set of [[SparkAppPerfReport]]s to aggregate
 */
class SparkAppPerfAggregateReport(val appPerfReports: Seq[SparkAppPerfReport])
    extends SparkAppPerfReportBase[JobName, StageName, AggregatePerfMetrics] {

  override protected def buildPerJobData: () => Seq[(JobName, Seq[(StageName, AggregatePerfMetrics)])] =
    () => new SparkAppReportMatcher(appPerfReports).matchingAppReports.map(perJobAggregatePerfMetrics)

  override protected def buildSummaryReport: () => AggregatePerfMetrics =
    () => AggregatePerfMetrics(appPerfReports.map(_.summaryReport))

  override protected def buildJobLevelReport: () => ListMap[JobName, AggregatePerfMetrics] =
    () => {
      var jobLevelData = ListMap[JobName, AggregatePerfMetrics]()
      if (perJobReport.isValid)
        new SparkAppReportMatcher(appPerfReports).matchingAppReports
          .foreach(jobLevelData += jobLevelAggregatePerfMetrics(_))
      jobLevelData
    }
}

object SparkAppPerfAggregateReport {
  type AggregatePerfMetrics = PerfMetrics[AggregatePerfValue]
  private def AggregatePerfMetrics(perfMetricsSeq: Seq[PerfMetrics[PerfValue]]): AggregatePerfMetrics = {
    val perfMetricsToAggregate = perfMetricsSeq.flatMap(_.keys).distinct
    perfMetricsToAggregate
      .map(
        perfMetric =>
          perfMetric ->
            new AggregatePerfValueImpl(perfMetricsSeq.map(_(perfMetric))))
      .toMap
  }

  private def perStageAggregatePerfMetrics(matchingStageMetrics: MatchingStageMetrics) =
    matchingStageMetrics.stageName -> AggregatePerfMetrics(matchingStageMetrics.stageMetricsSeq.map(_.perfMetrics))

  private def perJobAggregatePerfMetrics(matchingJobMetrics: MatchingJobMetrics) =
    matchingJobMetrics.jobName -> matchingJobMetrics.matchingStageMetricsSeq.map(perStageAggregatePerfMetrics)

  private def jobLevelAggregatePerfMetrics(matchingJobMetrics: MatchingJobMetrics) = {
    matchingJobMetrics.jobName -> AggregatePerfMetrics(accumulatedPerfMetrics(matchingJobMetrics))
  }

  private[utils] def accumulatedPerfMetrics(matchingJobMetrics: MatchingJobMetrics) = {
    for (i <- matchingJobMetrics.matchingStageMetricsSeq.head.stageMetricsSeq.indices)
      yield
        SparkAppPerfReport.accumulatedMetrics(
          matchingJobMetrics.matchingStageMetricsSeq.map(_.stageMetricsSeq(i).perfMetrics))
  }
}
