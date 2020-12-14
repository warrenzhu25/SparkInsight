package com.microsoft.spark.insight.utils.spark

import scala.collection.mutable.ArrayBuffer

/**
 * Some common String utils
 */
object CommonStringUtils {

  /**
   * Generate warnings if there is a possibility of inconsistent metrics
   * @param appReport a SparkAppPerfReport
   */
  def extractWarnings(appReport: SparkAppPerfReport): ArrayBuffer[String] = {

    val warnings = ArrayBuffer[String]()

    if (appReport.incompleteAppRun) warnings += INCOMPLETE_APP_RUN_WARNING

    if (appReport.unsuccessfulJobsExist) warnings += UNSUCCESSFUL_JOBS_EXIST_WARNING

    if (appReport.retriedStagesExist) warnings += RETRIED_STAGES_EXIST_WARNING

    warnings
  }

  /**
   * Warning string pattern
   * @param badAppReason Reason for the warning
   * @param inconsistencyReason root cause of inconsistent metrics
   * @return Warning String
   */
  def badAppWarning(badAppReason: String, inconsistencyReason: String): String = {
    s"This application run $badAppReason.\nThis may lead to inconsistent metrics reporting due to " +
      s"$inconsistencyReason. Metrics should be treated with caution.\n"
  }

  /**
   * Feedback link is added to this template in both cli and views.html modules.
   */
  val FEEDBACK_LINK_TEMPLATE: String = "Got feedback/requests? please share it with us at"

  /**
   * GridBench feedback link
   */
  val FEEDBACK_LINK: String = "http://go/gridbench/feedback"

  /**
   * GridBench go Link
   */
  val GRIDBENCH_LINK: String = "http://go/gridbench"

  /**
   * warning string for an incomplete app run
   */
  val INCOMPLETE_APP_RUN_WARNING: String =
    badAppWarning("wasn't marked as completed, and was possibly stopped or killed prematurely", "missing data")

  /**
   * warning string for an application containing unsuccessful jobs
   */
  val UNSUCCESSFUL_JOBS_EXIST_WARNING: String = badAppWarning("contains unsuccessful job(s)", "missing data")

  /**
   * warning string for application containing jobs with retried stages
   */
  val RETRIED_STAGES_EXIST_WARNING: String =
    badAppWarning("contains stage(s) with multiple attempts", "some tasks running more than once")

  /**
   * Warning String for diffReport with identical applications.
   */
  val IDENTICAL_APPS_WARNING: String = "The provided two appIds are identical."
}
