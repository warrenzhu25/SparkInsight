package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.spark.SparkAppPerfAggregateDiffReport._
import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher.{
  JobName,
  MatchingJobMetrics,
  MatchingStageMetrics,
  StageName
}
import com.microsoft.spark.insight.utils.{AggregatePerfValue, AggregatePerfValueImpl}

import scala.collection.immutable.ListMap

/**
 * A side-by-side comparison report between two sets of [[SparkAppPerfReport]]s. The report will try to match all of
 * the given app reports, and if they match, will produce an [[AggregateDiffPerfMetrics]] per every given matching
 * stageIds. The report will also produce an [[AggregateDiffPerfMetrics]] for the summaries of each app report
 *
 * @param appPerfReports1 application report set #1
 * @param appPerfReports2 application report set #2
 */
class SparkAppPerfAggregateDiffReport(
    appPerfReports1: Seq[SparkAppPerfReport],
    appPerfReports2: Seq[SparkAppPerfReport])
    extends SparkAppPerfReportBase[JobName, StageName, AggregateDiffPerfMetrics] {

  override protected def buildPerJobData: () => Seq[(JobName, Seq[(StageName, AggregateDiffPerfMetrics)])] = () => {
    val (appMatcher1, appMatcher2) = SparkAppReportMatcher.matchAppReportSets(appPerfReports1, appPerfReports2)
    (appMatcher1.matchingAppReports zip appMatcher2.matchingAppReports).map(perJobAggregateDiffMetrics)
  }

  override protected def buildSummaryReport: () => AggregateDiffPerfMetrics =
    () => AggregateDiffPerfMetrics(appPerfReports1.map(_.summaryReport), appPerfReports2.map(_.summaryReport))

  override protected def buildJobLevelReport: () => ListMap[JobName, AggregateDiffPerfMetrics] = () => {
    var jobLevelData = ListMap[JobName, AggregateDiffPerfMetrics]()
    if (perJobReport.isValid) {
      val (appMatcher1, appMatcher2) = SparkAppReportMatcher.matchAppReportSets(appPerfReports1, appPerfReports2)
      (appMatcher1.matchingAppReports zip appMatcher2.matchingAppReports)
        .foreach(jobLevelData += jobLevelAggregateDiffMetrics(_))
    }
    jobLevelData
  }
}

object SparkAppPerfAggregateDiffReport {
  case class AggregateDiffPerfValue(aggregatePerfValue1: AggregatePerfValue, aggregatePerfValue2: AggregatePerfValue)

  type AggregateDiffPerfMetrics = PerfMetrics[AggregateDiffPerfValue]
  private def AggregateDiffPerfMetrics(
      perfMetricsSeq1: Seq[PerfMetrics[PerfValue]],
      perfMetricsSeq2: Seq[PerfMetrics[PerfValue]]): AggregateDiffPerfMetrics = {
    val perfMetricsToAggregate = perfMetricsSeq1.flatMap(_.keys).distinct

    perfMetricsToAggregate
      .map(
        perfMetric =>
          perfMetric ->
            AggregateDiffPerfValue(
              new AggregatePerfValueImpl(perfMetricsSeq1.map(_(perfMetric))),
              new AggregatePerfValueImpl(perfMetricsSeq2.map(_(perfMetric)))))
      .toMap
  }

  private def perStageAggregateDiffMetrics(matchingStageMetricsPair: (MatchingStageMetrics, MatchingStageMetrics)) =
    matchingStageMetricsPair._1.stageName ->
      AggregateDiffPerfMetrics(
        matchingStageMetricsPair._1.stageMetricsSeq.map(_.perfMetrics),
        matchingStageMetricsPair._2.stageMetricsSeq.map(_.perfMetrics))

  private def perJobAggregateDiffMetrics(matchingJobMetricsPair: (MatchingJobMetrics, MatchingJobMetrics)) =
    matchingJobMetricsPair._1.jobName ->
      (matchingJobMetricsPair._1.matchingStageMetricsSeq zip matchingJobMetricsPair._2.matchingStageMetricsSeq)
        .map(perStageAggregateDiffMetrics)

  private def jobLevelAggregateDiffMetrics(matchingJobMetricsPair: (MatchingJobMetrics, MatchingJobMetrics)) = {
    matchingJobMetricsPair._1.jobName -> AggregateDiffPerfMetrics(
      SparkAppPerfAggregateReport.accumulatedPerfMetrics(matchingJobMetricsPair._1),
      SparkAppPerfAggregateReport.accumulatedPerfMetrics(matchingJobMetricsPair._2))
  }
}
