package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils._
import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.{JobId, StageId}
import com.microsoft.spark.insight.utils.spark.SparkAppPerfReport.StageIdAttemptId
import org.apache.spark.status.api.v1.{JobData, StageData}

import scala.collection.mutable
import scala.reflect.runtime.universe._

/**
 * A class that represents a set of structurally matching [[SparkAppPerfReport]]s, accompanied by the metrics collected
 * per-stage.
 *
 * @param appPerfReports to match
 */
class SparkAppReportMatcher(val appPerfReports: Seq[SparkAppPerfReport]) {
  import com.microsoft.spark.insight.utils.spark.SparkAppReportMatcher._

  private val _matchingAppReports: MatchingAppReports = MatchingAppReports(appPerfReports)

  /**
   * Retrieve the matching job -> stage -> metrics structure
   *
   * @return a sequence of [[MatchingJobMetrics]], which groups matching JobIds to the matching set of StageIds that
   *         belong to those jobs
   */
  def matchingAppReports: MatchingAppReports = _matchingAppReports
}

object SparkAppReportMatcher {

  /**
   * A pair of a [[StageId]] and its corresponding [[PerfMetrics]]
   *
   * @param stageId stageId for this stage
   * @param perfMetrics corresponding perfMetrics
   */
  case class StageMetrics(stageId: StageId, perfMetrics: PerfMetrics[PerfValue])

  object StageMetrics {
    private[SparkAppReportMatcher] def apply(stageAttrMetrics: StageAttrMetrics): StageMetrics =
      StageMetrics(stageAttrMetrics.stageAttr.stageId, stageAttrMetrics.perfMetrics)
  }

  /**
   * A collection of [[StageMetrics]] from matching stages that share the same [[StageName]] and [[StageDetails]]
   *
   * @param stageName stage name from [[StageData]]
   * @param stageDetails stage details form [[StageData]]
   * @param stageMetricsSeq a sequence of StageMetrics
   */
  case class MatchingStageMetrics(
      stageName: StageName,
      stageDetails: StageDetails,
      stageMetricsSeq: Seq[StageMetrics]) {
    private[SparkAppReportMatcher] def ++(that: MatchingStageMetrics): MatchingStageMetrics = {
      assume(this.stageName.equals(that.stageName), "Trying to merge mismatching stages")
      assume(this.stageDetails.equals(that.stageDetails), "Trying to merge mismatching stages")
      MatchingStageMetrics(this.stageName, this.stageDetails, this.stageMetricsSeq ++ that.stageMetricsSeq)
    }
  }

  object MatchingStageMetrics {
    private[SparkAppReportMatcher] def apply(stageAttrMetrics: StageAttrMetrics): MatchingStageMetrics =
      MatchingStageMetrics(
        stageAttrMetrics.stageAttr.stageName,
        stageAttrMetrics.stageAttr.details,
        Seq(StageMetrics(stageAttrMetrics)))
  }

  /**
   * A wrapper for a sequence of [[MatchingStageMetrics]], used for allowing a custom "++" operation for merging
   *
   * @param matchingStageMetricsSeq sequence to wrap
   */
  class MatchingStageMetricsSeq(private val matchingStageMetricsSeq: Seq[MatchingStageMetrics])
      extends Seq[MatchingStageMetrics] {
    override def length: Int = matchingStageMetricsSeq.length

    override def apply(idx: Int): MatchingStageMetrics = matchingStageMetricsSeq.apply(idx)

    override def iterator: Iterator[MatchingStageMetrics] = matchingStageMetricsSeq.iterator

    private[SparkAppReportMatcher] def ++(that: MatchingStageMetricsSeq): MatchingStageMetricsSeq =
      new MatchingStageMetricsSeq((this zip that).map {
        case (stageMetricsSeq1, stageMetricsSeq2) => stageMetricsSeq1 ++ stageMetricsSeq2
      })
  }

  object MatchingStageMetricsSeq {
    private[SparkAppReportMatcher] def apply(stageAttrsMetrics: StageAttrsMetrics) = {
      // Rename duplicates
      val renamedStages = renameDuplicates(stageAttrsMetrics.map(_.stageAttr.stageName))
      // Apply the new stage names
      renamedStages.zip(stageAttrsMetrics).foreach {
        case (newName, stageAttrMetrics) => stageAttrMetrics.stageAttr.stageName = newName
      }

      new MatchingStageMetricsSeq(stageAttrsMetrics.map(MatchingStageMetrics(_)))
    }
  }

  type MatchingJobIds = Seq[JobId]

  /**
   * A collection of [[MatchingStageMetricsSeq]] from matching jobs that share the same [[JobName]]
   *
   * @param jobName jobName from [[JobData]]
   * @param matchingJobIds the matching JobIds represented in this collection
   * @param matchingStageMetricsSeq matchingStageMetricsSeq that are ordered in the same way as the matchingJobIds
   */
  case class MatchingJobMetrics(
      jobName: JobName,
      matchingJobIds: MatchingJobIds,
      matchingStageMetricsSeq: MatchingStageMetricsSeq) {
    private[SparkAppReportMatcher] def ++(that: MatchingJobMetrics): MatchingJobMetrics = {
      assume(this.jobName.equals(that.jobName), "Trying to merge mismatching jobs")
      MatchingJobMetrics(
        this.jobName,
        this.matchingJobIds ++ that.matchingJobIds,
        this.matchingStageMetricsSeq ++ that.matchingStageMetricsSeq)
    }
  }

  object MatchingJobMetrics {
    private[SparkAppReportMatcher] def apply(jobAttrMetrics: JobAttrMetrics): MatchingJobMetrics =
      MatchingJobMetrics(
        jobAttrMetrics.jobAttr.jobName,
        Seq(jobAttrMetrics.jobAttr.jobId),
        MatchingStageMetricsSeq(jobAttrMetrics.stageAttrsMetrics))
  }

  type MatchingAppReports = Seq[MatchingJobMetrics]
  private def MatchingAppReports(appPerfReports: Seq[SparkAppPerfReport]): MatchingAppReports = {
    // Map the given appReports to include more attributes for Jobs and Stages that are needed for matching them
    val jobAttrsMetricsPerApp = appPerfReports.map(JobAttrsMetrics)

    // Verify that all of the apps match the first app, hence all of them match
    jobAttrsMetricsPerApp.tail.foreach(verifyMatchingApps(jobAttrsMetricsPerApp.head, _))

    // Map JobAttrs to a per-app MatchingJobsMetrics
    val matchingJobMetricsPerApp: Seq[Seq[MatchingJobMetrics]] = jobAttrsMetricsPerApp.map(jobAttrsMetrics => {
      // Rename duplicates
      val renamedJobs = renameDuplicates(jobAttrsMetrics.map(_.jobAttr.jobName))
      // Apply the new job names
      renamedJobs.zip(jobAttrsMetrics).foreach {
        case (newName, jobAttrMetrics) => jobAttrMetrics.jobAttr.jobName = newName
      }

      jobAttrsMetrics.map(MatchingJobMetrics(_))
    })

    // Reduce per-app MatchingJobMetrics into a single combined set of MatchingJobMetrics
    for (i <- 0 until matchingJobMetricsPerApp.map(_.length).min) yield {
      matchingJobMetricsPerApp.map(_(i)).reduce(_ ++ _)
    }
  }

  private abstract class UniqueAttr[T] {
    def uniqueAttr: T
  }

  private object UniqueAttr {
    /*
     * A loose ordering of JobDatas and StageDatas. Used for sorting and for matching jobs and stages between
     * applications. Spark does not expose any concrete unique IDs that can be used to compare two runs. A more precise
     * solution can be achieved by comparing DAGs, but those are not accessible by REST API, and will require manual
     * parsing of Spark's web UI or raw logs. The below was found to be accurate enough to distinguish between different
     * applications, but may result in a false rejection of apps
     */
    implicit def ordering[A <: UniqueAttr[_]: TypeTag]: Ordering[A] = typeOf[A] match {
      case t if t <:< typeOf[UniqueAttr[(String, String)]] =>
        Ordering.by(_.asInstanceOf[UniqueAttr[(String, String)]].uniqueAttr)
      case t if t <:< typeOf[UniqueAttr[(String, Int)]] =>
        Ordering.by(_.asInstanceOf[UniqueAttr[(String, Int)]].uniqueAttr)
    }
  }

  type JobName = String
  type StageName = String
  type StageDetails = String

  private case class JobAttr(jobId: JobId, var jobName: JobName, numStages: Int) extends UniqueAttr[(JobName, Int)] {
    override def uniqueAttr: (JobName, Int) = (jobName, numStages)
  }

  private object JobAttr {
    def apply(jobData: JobData): JobAttr = JobAttr(jobData.jobId, jobData.name, jobData.stageIds.size)
  }

  private case class StageAttr(stageId: StageId, var stageName: StageName, details: StageDetails)
      extends UniqueAttr[(StageName, StageDetails)] {
    override def uniqueAttr: (StageName, StageDetails) = (stageName, details)
  }

  private object StageAttr {
    def apply(stageData: StageData): StageAttr = StageAttr(stageData.stageId, stageData.name, stageData.details)
  }

  private case class JobAttrMetrics(jobAttr: JobAttr, stageAttrsMetrics: StageAttrsMetrics)
  private type JobAttrsMetrics = Seq[JobAttrMetrics]
  private def JobAttrsMetrics(appPerfReport: SparkAppPerfReport): JobAttrsMetrics = {
    if (appPerfReport.incompleteAppRun) {
      throw new IllegalArgumentException(
        s"Incomplete app run detected (${appPerfReport.rawSparkMetrics.appId}), internal matching of stages is " +
          s"not supported. Such applications should not be used for comparing metrics as data may be incomplete.")
    }
    if (appPerfReport.unsuccessfulJobsExist) {
      throw new IllegalArgumentException(
        s"App with unsuccessful jobs detected (${appPerfReport.rawSparkMetrics.appId}), internal matching of " +
          s"stages is not supported. Such applications should not be used for comparing metrics as data may be " +
          s"incomplete.")
    }
    if (appPerfReport.retriedStagesExist) {
      throw new IllegalArgumentException(
        s"App with multiple stage attempts detected (${appPerfReport.rawSparkMetrics.appId}), internal " +
          s"matching of stages is not supported. Such applications should not be used for comparing metrics as " +
          s"they may incorrectly report duplicate task runs.")
    }

    appPerfReport.perJobReport
      .map {
        case (jobId, perStageReport) =>
          JobAttrMetrics(
            JobAttr(appPerfReport.rawSparkMetrics.firstAppAttempt.jobDataMap(jobId)),
            StageAttrsMetrics(appPerfReport, perStageReport))
      }
      .toSeq
      .sortBy(_.jobAttr)
  }

  private case class StageAttrMetrics(stageAttr: StageAttr, perfMetrics: PerfMetrics[PerfValue])
  private type StageAttrsMetrics = Seq[StageAttrMetrics]
  private def StageAttrsMetrics(
      appPerfReport: SparkAppPerfReport,
      perStageReport: PerStageReport[StageIdAttemptId, PerfMetrics[PerfValue]]): StageAttrsMetrics =
    perStageReport
      .map {
        case (stageIdAttemptId, perfMetrics) =>
          StageAttrMetrics(
            StageAttr(
              appPerfReport.rawSparkMetrics.firstAppAttempt.stageDataMap(stageIdAttemptId.stageId)(
                StageIdAttemptId.FIRST_STAGE_ATTEMPT_ID)),
            perfMetrics)
      }
      .toSeq
      .sortBy(_.stageAttr)

  private def verifyMatchingApps(jobAttrsMetrics1: JobAttrsMetrics, jobAttrsMetrics2: JobAttrsMetrics) {
    (jobAttrsMetrics1 zip jobAttrsMetrics2).foreach {
      case (jobAttrMetrics1, jobAttrMetrics2) =>
        if (!jobAttrMetrics1.jobAttr.uniqueAttr.equals(jobAttrMetrics2.jobAttr.uniqueAttr)) {
          throw new IllegalArgumentException(
            "Apps with mismatching job lists detected, unable to produce a per-job matching for the apps. " +
              s"App #1: ${jobAttrMetrics1.jobAttr.jobId}; App #2: ${jobAttrMetrics2.jobAttr.jobId}")
        }

        Seq(jobAttrMetrics1.stageAttrsMetrics, jobAttrMetrics2.stageAttrsMetrics)
          .map(_.map(_.stageAttr.uniqueAttr))
          .distinct
          .length match {
          case 1 =>
          case _ =>
            throw new IllegalArgumentException(
              "Apps with mismatching stage lists detected, unable to " +
                s"produce a per-stage matching for the apps. Stage list for app #1: ${jobAttrMetrics1.stageAttrsMetrics
                  .map(_.stageAttr.stageId)
                  .mkString(", ")}; stage list for app #2: ${jobAttrMetrics2.stageAttrsMetrics
                  .map(_.stageAttr.stageId)
                  .mkString(", ")}")
        }
    }
  }

  private def renameDuplicates(originalNames: Seq[String]): Seq[String] = {
    val repetitionsMap = new mutable.HashMap[String, Int]()

    originalNames.map(originalName => {
      val curRepetitions = repetitionsMap.getOrElse(originalName, 0)
      repetitionsMap.put(originalName, curRepetitions + 1)

      if (curRepetitions > 0) {
        originalName + s" #$curRepetitions"
      } else {
        originalName
      }
    })
  }

  /**
   * Constructs a pair of [[SparkAppReportMatcher]]s from the provided pair of [[SparkAppPerfReport]]s sets IFF all of
   * the [[SparkAppPerfReport]]s is each sets match
   *
   * @param appPerfReports1 Spark app perf report set #1
   * @param appPerfReports2 Spark app perf report set #2
   * @return A pair of matchers
   */
  def matchAppReportSets(
      appPerfReports1: Seq[SparkAppPerfReport],
      appPerfReports2: Seq[SparkAppPerfReport]): (SparkAppReportMatcher, SparkAppReportMatcher) = {
    try {
      // Check if the first app report from each set match each other. A later check will verify that all of the app
      // reports within each set match, and since matching is transitive, that will be identical to matching both of the
      // complete sets
      new SparkAppReportMatcher(Seq(appPerfReports1.head, appPerfReports2.head))
    } catch {
      case iae: IllegalArgumentException =>
        throw new IllegalArgumentException("Mismatching sets of app reports", iae)
    }

    val appMatcher1 =
      try {
        new SparkAppReportMatcher(appPerfReports1)
      } catch {
        case iae: IllegalArgumentException =>
          throw new IllegalArgumentException("The first set of app reports does match internally", iae)
      }

    val appMatcher2 =
      try {
        new SparkAppReportMatcher(appPerfReports2)
      } catch {
        case iae: IllegalArgumentException =>
          throw new IllegalArgumentException("The second set of app reports does match internally", iae)
      }

    (appMatcher1, appMatcher2)
  }
}
