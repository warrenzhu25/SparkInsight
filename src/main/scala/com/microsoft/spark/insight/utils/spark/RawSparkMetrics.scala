package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils.spark.RawSparkMetrics.AppAttemptId
import org.apache.spark.sql.Row
import org.apache.spark.status.api.v1.ApplicationInfo

/**
 * Container class that collects all of the raw data collected from SHS for a specific Spark application
 *
 * @param appInfo General application info and application attempts
 * @param appAttemptMap a mapping from an application attemptId to a [[RawSparkApplicationAttempt]]
 */
case class RawSparkMetrics(appInfo: ApplicationInfo, appAttemptMap: Map[AppAttemptId, RawSparkApplicationAttempt]) {
  import RawSparkMetrics._

  /**
   * Retrieve the first App attempt
   *
   * @return a RawSparkApplicationAttempt object
   */
  def firstAppAttempt: RawSparkApplicationAttempt = appAttemptMap.get(FIRST_APP_ATTEMPT) match {
    case Some(rawSparkApplicationAttempt) => rawSparkApplicationAttempt
    case None => throw new IllegalArgumentException(s"Unable to retrieve application attempt $FIRST_APP_ATTEMPT")
  }

  /**
   * Retrieve the applicationId for this app run
   *
   * @return string of appId
   */
  def appId: String = appInfo.id
}

object RawSparkMetrics {
  type AppAttemptId = String
  val FIRST_APP_ATTEMPT: AppAttemptId = "1"

  def apply(sparkMetricsRow: Row): RawSparkMetrics = SparkMetricsRowConversionUtils.convert(sparkMetricsRow)
}
