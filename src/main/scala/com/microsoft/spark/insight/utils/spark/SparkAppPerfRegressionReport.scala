package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.RegressionAnalysis
import com.microsoft.spark.insight.utils.spark.SparkAppPerfRegressionReport._
import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher.{
  JobName,
  MatchingJobMetrics,
  MatchingStageMetrics,
  StageName
}

import scala.collection.immutable.ListMap

/**
 * A Performance Regression Analysis report for the given pair of [[SparkAppPerfReport]] sets.
 * This class will verify whether the internal structures of the applications match, and if so, will produce a
 * per-stage regression analysis. If the applications don't match, then only a summarized view will be provided.
 *
 * @param appPerfReports1 Spark app perf report set #1 (e.g. "before")
 * @param appPerfReports2 Spark app perf report set #2 (e.g. "after")
 */
class SparkAppPerfRegressionReport(
    val appPerfReports1: Seq[SparkAppPerfReport],
    val appPerfReports2: Seq[SparkAppPerfReport])
    extends SparkAppPerfReportBase[JobName, StageName, RegressionAnalysisPerfMetrics] {

  override protected def buildPerJobData: () => Seq[(JobName, Seq[(StageName, RegressionAnalysisPerfMetrics)])] =
    () => {
      val (appMatcher1, appMatcher2) = SparkAppReportMatcher.matchAppReportSets(appPerfReports1, appPerfReports2)
      (appMatcher1.matchingAppReports zip appMatcher2.matchingAppReports).map(perJobRegressionAnalysis)
    }

  override protected def buildSummaryReport: () => RegressionAnalysisPerfMetrics =
    () => RegressionAnalysisPerfMetrics(appPerfReports1.map(_.summaryReport), appPerfReports2.map(_.summaryReport))

  override protected def buildJobLevelReport: () => ListMap[JobName, RegressionAnalysisPerfMetrics] = () => {
    var jobLevelData = ListMap[JobName, RegressionAnalysisPerfMetrics]()
    if (perJobReport.isValid) {
      val (appMatcher1, appMatcher2) = SparkAppReportMatcher.matchAppReportSets(appPerfReports1, appPerfReports2)
      (appMatcher1.matchingAppReports zip appMatcher2.matchingAppReports)
        .foreach(jobLevelData += jobLevelRegressionAnalysis(_))
    }
    jobLevelData
  }
}

object SparkAppPerfRegressionReport {
  type RegressionAnalysisPerfMetrics = PerfMetrics[RegressionAnalysis]
  private def RegressionAnalysisPerfMetrics(
      perfMetricsSeq1: Seq[PerfMetrics[PerfValue]],
      perfMetricsSeq2: Seq[PerfMetrics[PerfValue]]): RegressionAnalysisPerfMetrics = {
    // Only analyze perfMetrics that should be used in regression reports
    val perfMetricsToAnalyze = perfMetricsSeq1.flatMap(_.keys).distinct.filter(_.useInRegressionReport)

    perfMetricsToAnalyze
      .map(
        perfMetric =>
          perfMetric ->
            new RegressionAnalysis(perfMetricsSeq1.map(_(perfMetric)), perfMetricsSeq2.map(_(perfMetric)), perfMetric))
      .toMap
  }

  /**
   * 'executorCpuTimeMs' values were found to be distributing normally across different application runs, so they are
   * the go-to metric for measuring performance regression of Spark apps
   */
  val DEFAULT_REGRESSION_METRIC: PerfMetric = PerfMetric.EXECUTOR_CPU_TIME

  private def perStageRegressionAnalysis(matchingStageMetricsPair: (MatchingStageMetrics, MatchingStageMetrics)) = {
    matchingStageMetricsPair._1.stageName ->
      RegressionAnalysisPerfMetrics(
        matchingStageMetricsPair._1.stageMetricsSeq.map(_.perfMetrics),
        matchingStageMetricsPair._2.stageMetricsSeq.map(_.perfMetrics))
  }

  private def perJobRegressionAnalysis(matchingJobMetricsPair: (MatchingJobMetrics, MatchingJobMetrics)) = {
    matchingJobMetricsPair._1.jobName ->
      (matchingJobMetricsPair._1.matchingStageMetricsSeq zip matchingJobMetricsPair._2.matchingStageMetricsSeq)
        .map(perStageRegressionAnalysis)
  }

  private def jobLevelRegressionAnalysis(matchingJobMetricsPair: (MatchingJobMetrics, MatchingJobMetrics)) = {
    matchingJobMetricsPair._1.jobName -> RegressionAnalysisPerfMetrics(
      SparkAppPerfAggregateReport.accumulatedPerfMetrics(matchingJobMetricsPair._1),
      SparkAppPerfAggregateReport.accumulatedPerfMetrics(matchingJobMetricsPair._2))
  }
}
