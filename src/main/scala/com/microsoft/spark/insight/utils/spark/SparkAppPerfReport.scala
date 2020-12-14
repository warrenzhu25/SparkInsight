package com.microsoft.spark.insight.utils.spark

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}
import java.util.Date
import java.util.concurrent.TimeUnit

import com.microsoft.spark.insight.utils.PerfMetric._
import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.{AttemptId, JobId, StageId}
import com.microsoft.spark.insight.utils.spark.RawSparkMetrics.FIRST_APP_ATTEMPT
import com.microsoft.spark.insight.utils.spark.SparkAppPerfReport.StageIdAttemptId
import org.apache.commons.lang3.builder.{ReflectionToStringBuilder, ToStringStyle}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.{ApplicationInfo, JobData, StageData}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap

/**
 * A Spark application performance report - takes a [[RawSparkMetrics]] and extracts/transforms specific metrics per
 * job, per stage and another summarized view of the entire application
 *
 * @param rawSparkMetrics Raw Spark metrics retrieved from a [[SparkMetricsRetriever]]
 */
class SparkAppPerfReport(val rawSparkMetrics: RawSparkMetrics)
    extends SparkAppPerfReportBase[JobId, StageIdAttemptId, PerfMetrics[PerfValue]] {
  import SparkAppPerfReport._

  private var _incompleteAppRun: Boolean = _
  private var _unsuccessfulJobsExist: Boolean = _
  private var _retriedStagesExist: Boolean = _

  /**
   * Return true/false whether this app run was marked as completed
   *
   * @return true iff app run is incomplete
   */
  def incompleteAppRun: Boolean = _incompleteAppRun

  /**
   * Return true/false whether unsuccessful jobs exist in this application run
   *
   * @return true iff unsuccessful jobs were detected for this run
   */
  def unsuccessfulJobsExist: Boolean = _unsuccessfulJobsExist

  /**
   * Return true/false whether stages with multiple attempts exist in this application run
   *
   * @return true iff stages with multiple attempts were detected for this run
   */
  def retriedStagesExist: Boolean = _retriedStagesExist

  override protected def buildPerJobData: () => Seq[(JobId, Seq[(StageIdAttemptId, PerfMetrics[PerfValue])])] =
    () => {
      // Extract and validate the first app attempt
      val appAttempt = extractAppAttempt(rawSparkMetrics)
      val stageDataMap = appAttempt.stageDataMap

      _incompleteAppRun = !appAttempt.appAttemptInfo.completed

      // Check whether there were stages with more than a single attempt
      _retriedStagesExist = stageDataMap.exists(_._2.size > 1)

      appAttempt.jobDataMap.values
        .map(jobData => {
          _unsuccessfulJobsExist |= !isSuccessfulJobData(jobData)
          // Extract the stages corresponding to this jobId
          val stageDatas = stageDataMap.filterKeys(jobData.stageIds.toSet).values.flatMap(_.values)
          jobData.jobId -> perStagePerfMetrics(stageDatas)
        })
        .toSeq
    }

  // SparkAppPerfReport should throw an exception if the rawMetrics are invalid
  if (!perJobReport.isValid) {
    throw perJobReport.failure
  }

  if (_retriedStagesExist) {
    LOGGER.warn(
      s"${rawSparkMetrics.appId} contains stages with multiple attempts. This can severely affect " +
        s"the metrics that were recorded, hence comparing it to other runs should be avoided.")
  }

  override protected def buildSummaryReport: () => PerfMetrics[PerfValue] = () => {
    if (perJobReport.isValid) {
      // Sum-up all of the 'accumulable' metrics across all stages
      val accumlablePerfMetrics = accumulatedMetrics(perJobReport.allJobs.flatMap(_.allStages))

      // Produce the summary-specific metrics
      val summaryPerfMetrics = {
        val rawSparkApplicationAttempt = extractAppAttempt(rawSparkMetrics)
        summaryPerfMetricsMethods.map(_.apply(rawSparkApplicationAttempt)).toMap
      }

      accumlablePerfMetrics ++ summaryPerfMetrics
    } else {
      Map.empty
    }
  }

  override protected def buildJobLevelReport: () => ListMap[JobId, PerfMetrics[PerfValue]] = () => {
    var jobLevelData = ListMap[JobId, PerfMetrics[PerfValue]]()
    if (perJobReport.isValid) {
      for (jobKey <- perJobReport.jobKeys) {
        jobLevelData += (jobKey -> accumulatedMetrics(perJobReport.job(jobKey).allStages))
      }
    }
    jobLevelData
  }
}

object SparkAppPerfReport {
  val LOGGER: Logger = LoggerFactory.getLogger(classOf[SparkAppPerfReport])

  /**
   * A pair of a StageId and a stage AttemptId. Used as a StageKey to be able to distinguish different attempts. This
   * type of report is the only one to utilize this type as it only represents a single application. Other reports
   * perform matching between several [SparkAppPerfReport]s, and matching is designed to fail in the stage-level for
   * runs with multiple stage attempts, as those may severely skew per-stage metrics
   *
   * @param stageId stageId
   * @param attemptId attemptId
   */
  case class StageIdAttemptId(stageId: StageId, attemptId: AttemptId) {
    override def toString: String = s"StageId: $stageId, AttemptId: $attemptId"
  }

  object StageIdAttemptId {

    /** The first attempt ID used in Spark apps */
    val FIRST_STAGE_ATTEMPT_ID = 0

    /**
     * A convenience method for creating a [[StageIdAttemptId]] for a stageId with the [[FIRST_STAGE_ATTEMPT_ID]]
     *
     * @param stageId stageId
     * @return stageIdAttemptId with the provided stageId and attemptId set to [[FIRST_STAGE_ATTEMPT_ID]]
     */
    def apply(stageId: StageId): StageIdAttemptId = StageIdAttemptId(stageId, FIRST_STAGE_ATTEMPT_ID)
  }

  private def PerfMetrics(stageData: StageData): PerfMetrics[PerfValue] =
    perStagePerfMetricsMethods.map(_.apply(stageData)).toMap
  private def combineOp(
      perfMetrics1: PerfMetrics[PerfValue],
      perfMetrics2: PerfMetrics[PerfValue]): PerfMetrics[PerfValue] = {
    perfMetrics1 ++ perfMetrics2.map { case (k, v) => k -> (v + perfMetrics1.getOrElse(k, 0L)) }
  }

  private def perStagePerfMetrics(stageDatas: Iterable[StageData]) =
    stageDatas
      .map(stageData => StageIdAttemptId(stageData.stageId, stageData.attemptId) -> PerfMetrics(stageData))
      .toSeq

  private def verifyValidAppInfo(appInfo: ApplicationInfo) {
    if (appInfo.attempts.size != 1) {
      throw new IllegalArgumentException(s"Invalid Spark app detected: (${appInfo.id}) - contains " +
        s"${appInfo.attempts.size} attempts. Report creation for applications with more than a single attempt is not" +
        s" supported")
    }

    val firstAttempt = appInfo.attempts.head
    firstAttempt.attemptId match {
      case Some(attemptId) if !attemptId.equals(FIRST_APP_ATTEMPT) =>
        throw new IllegalArgumentException(
          s"Invalid Spark app detected: (${appInfo.id}) - contains an unexpected application attempt id. Actual: " +
            s"$attemptId, expected: $FIRST_APP_ATTEMPT")
      case None =>
        throw new IllegalArgumentException(
          s"Invalid Spark app detected: (${appInfo.id}) - contains an undefined attemptId")
      case _ =>
    }

    if (!firstAttempt.completed) {
      LOGGER.warn(s"Incomplete Spark app detected: (${appInfo.id}) - application is still running")
    }
  }

  /**
   * Accumulate all the accumulable metrics into a single PerfMetricsType
   *
   * @param allStagesMetrics corresponding Iterable Seq of PerfMetricsType
   * @return accumulated PerfMetricsType
   */
  def accumulatedMetrics(allStagesMetrics: Iterable[PerfMetrics[PerfValue]]): PerfMetrics[PerfValue] =
    allStagesMetrics.map(_.filter(_._1.accumulable)).reduce(combineOp)

  /**
   * Extract the first Application Attempt raw metrics from a spark application.
   *
   * @param rawSparkMetrics raw data collected from SHS for a specific Spark application
   * @return [[RawSparkApplicationAttempt]] for an application attempt
   */
  def extractAppAttempt(rawSparkMetrics: RawSparkMetrics): RawSparkApplicationAttempt = {
    verifyValidAppInfo(rawSparkMetrics.appInfo)
    rawSparkMetrics.firstAppAttempt
  }

  private def isSuccessfulJobData(jobData: JobData): Boolean = {
    if (!jobData.status.equals(JobExecutionStatus.SUCCEEDED)) {
      LOGGER.warn(s"Unsuccessful job detected: \n${objectDebugString(jobData)}")
      false
    } else {
      true
    }
  }

  /*
   * Construct debug strings from all of the members of an object (used for printing classes that doesn't have an
   *  overloaded toString method)
   */
  private def objectDebugString(obj: Any): String =
    ReflectionToStringBuilder.toString(obj, ToStringStyle.MULTI_LINE_STYLE)

  private def localDate(date: Date): LocalDateTime = date.toInstant.atZone(ZoneId.systemDefault).toLocalDateTime

  private def diffDatesInMillis(earlierDate: Option[Date], laterDate: Option[Date]): Long = {
    (earlierDate, laterDate) match {
      case (Some(date1), Some(date2)) => ChronoUnit.MILLIS.between(localDate(date1), localDate(date2))
      case _ => 0L
    }
  }

  private def nanosToMillis(data: Long): Long = TimeUnit.NANOSECONDS.toMillis(data)

  /*
   * A collection of methods to extract and/or transform specific metrics from stageDatas
   */
  private val perStagePerfMetricsMethods: Seq[StageData => (PerfMetric, PerfValue)] = List(
    stageData => TOTAL_RUNNING_TIME -> diffDatesInMillis(stageData.submissionTime, stageData.completionTime),
    stageData =>
      SUBMISSION_TO_FIRST_LAUNCH_DELAY -> diffDatesInMillis(stageData.submissionTime, stageData.firstTaskLaunchedTime),
    stageData =>
      FIRST_LAUNCH_TO_COMPLETED -> diffDatesInMillis(stageData.firstTaskLaunchedTime, stageData.completionTime),
    stageData =>
      EXECUTOR_RUNTIME_LESS_SHUFFLE -> (stageData.executorRunTime - stageData.shuffleFetchWaitTime - nanosToMillis(
        stageData.shuffleWriteTime)),
    stageData => EXECUTOR_RUNTIME -> stageData.executorRunTime,
    stageData => EXECUTOR_CPU_TIME -> nanosToMillis(stageData.executorCpuTime),
    stageData => JVM_GC_TIME -> stageData.jvmGcTime,
    stageData => SHUFFLE_READ_FETCH_WAIT_TIME -> stageData.shuffleFetchWaitTime,
    stageData => SHUFFLE_WRITE_TIME -> nanosToMillis(stageData.shuffleWriteTime),
    stageData => INPUT_SIZE -> stageData.inputBytes,
    stageData => INPUT_RECORDS -> stageData.inputRecords,
    stageData => OUTPUT_SIZE -> stageData.outputBytes,
    stageData => OUTPUT_RECORDS -> stageData.outputRecords,
    stageData => SHUFFLE_READ_SIZE -> stageData.shuffleReadBytes,
    stageData => SHUFFLE_READ_RECORDS -> stageData.shuffleReadRecords,
    stageData => SHUFFLE_WRITE_SIZE -> stageData.shuffleWriteBytes,
    stageData => SHUFFLE_WRITE_RECORDS -> stageData.shuffleWriteRecords,
    stageData => MEM_SPILLS -> stageData.memoryBytesSpilled,
    stageData => DISK_SPILLS -> stageData.diskBytesSpilled,
    stageData =>
      NET_IO_TIME -> (stageData.executorRunTime - stageData.shuffleFetchWaitTime - nanosToMillis(
        stageData.shuffleWriteTime) - nanosToMillis(stageData.executorCpuTime) - stageData.jvmGcTime))

  private[spark] val summaryPerfMetricsMethods: Seq[RawSparkApplicationAttempt => (PerfMetric, PerfValue)] = List(
    rawSparkApplicationAttempt => TOTAL_RUNNING_TIME -> rawSparkApplicationAttempt.appAttemptInfo.duration)
}
