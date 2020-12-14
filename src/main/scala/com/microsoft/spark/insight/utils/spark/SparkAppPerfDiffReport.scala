package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.StageId
import com.microsoft.spark.insight.utils.spark.SparkAppPerfDiffReport.{MatchingStageIds, PerfMetricsDiff}
import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher.{
  MatchingJobIds,
  MatchingJobMetrics,
  MatchingStageMetricsSeq
}

import scala.collection.immutable.ListMap

/**
 * A side-by-side comparison report between two [[SparkAppPerfReport]]s. The included logic will analyze both of the
 * application reports to determine whether there is a clear matching between their underlying jobs and stages, and
 * will produce a table with both combining the metric values from both apps.
 *
 * @param appPerfReport1 application report #1
 * @param appPerfReport2 application report #2
 */
class SparkAppPerfDiffReport(val appPerfReport1: SparkAppPerfReport, val appPerfReport2: SparkAppPerfReport)
    extends SparkAppPerfReportBase[MatchingJobIds, MatchingStageIds, PerfMetricsDiff] {
  import com.microsoft.spark.insight.utils.spark.SparkAppPerfDiffReport._

  override protected def buildPerJobData: () => Seq[(MatchingJobIds, Seq[(MatchingStageIds, PerfMetricsDiff)])] =
    () => perJobPerfMetricsDiff(appPerfReport1, appPerfReport2)

  override protected def buildSummaryReport: () => PerfMetricsDiff =
    () => PerfMetricsDiff(appPerfReport1.summaryReport, appPerfReport2.summaryReport)

  override protected def buildJobLevelReport: () => ListMap[MatchingJobIds, PerfMetricsDiff] =
    () => {
      var jobLevelData = ListMap[MatchingJobIds, PerfMetricsDiff]()
      if (perJobReport.isValid) {
        new SparkAppReportMatcher(Seq(appPerfReport1, appPerfReport2)).matchingAppReports
          .foreach(jobLevelData += jobLevelMetricsDiff(_))
      }
      jobLevelData
    }
}

object SparkAppPerfDiffReport {
  private def perJobPerfMetricsDiff(appPerfReport1: SparkAppPerfReport, appPerfReport2: SparkAppPerfReport) =
    new SparkAppReportMatcher(Seq(appPerfReport1, appPerfReport2)).matchingAppReports.map(matchingJobMetrics =>
      matchingJobMetrics.matchingJobIds -> perStagePerfMetricsDiff(matchingJobMetrics.matchingStageMetricsSeq))

  private def perStagePerfMetricsDiff(matchingStageMetricsSeq: MatchingStageMetricsSeq) = {
    matchingStageMetricsSeq.map(
      matchingStageMetrics =>
        MatchingStageIds(
          matchingStageMetrics.stageMetricsSeq.head.stageId,
          matchingStageMetrics.stageMetricsSeq.last.stageId) ->
          PerfMetricsDiff(
            matchingStageMetrics.stageMetricsSeq.head.perfMetrics,
            matchingStageMetrics.stageMetricsSeq.last.perfMetrics))
  }

  private def jobLevelMetricsDiff(matchingJobMetrics: MatchingJobMetrics) = {
    matchingJobMetrics.matchingJobIds -> PerfMetricsDiff(
      SparkAppPerfReport.accumulatedMetrics(
        matchingJobMetrics.matchingStageMetricsSeq.map(_.stageMetricsSeq.head.perfMetrics)),
      SparkAppPerfReport.accumulatedMetrics(
        matchingJobMetrics.matchingStageMetricsSeq.map(_.stageMetricsSeq.last.perfMetrics)))
  }

  type PerfMetricsDiff = PerfMetrics[MatchingPerfValues]
  private def PerfMetricsDiff(
      perfMetrics1: PerfMetrics[PerfValue],
      perfMetrics2: PerfMetrics[PerfValue]): PerfMetricsDiff = {
    perfMetrics1.keySet
      .intersect(perfMetrics2.keySet)
      .map(perfMetric => perfMetric -> MatchingPerfValues(perfMetrics1(perfMetric), perfMetrics2(perfMetric)))
      .toMap
  }

  /**
   * A pair of matching stages, denoted by their respective [[StageId]]s
   *
   * @param stageId1 stageId for stage #1
   * @param stageId2 stageId for stage #2
   */
  case class MatchingStageIds(stageId1: StageId, stageId2: StageId)

  /**
   * A pair of [[PerfValue]]s matching the same [[PerfMetric]]
   *
   * @param perfValue1 metric value #1
   * @param perfValue2 metric value #2
   */
  case class MatchingPerfValues(perfValue1: PerfValue, perfValue2: PerfValue)
}
