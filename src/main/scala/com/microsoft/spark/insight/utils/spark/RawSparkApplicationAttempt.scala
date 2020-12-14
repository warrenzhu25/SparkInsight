package com.microsoft.spark.insight.utils.spark

import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.{JobDataMap, StageDataMap}
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationEnvironmentInfo, JobData, StageData}

/**
 * Container for all of the raw metrics associated with a specific Spark Application Attempt
 *
 * @param appAttemptInfo attempt info from [[org.apache.spark.status.api.v1.ApplicationInfo]]
 * @param appEnvInfo environment info
 * @param jobDataMap a mapping from a jobId to its jobData
 * @param stageDataMap a mapping from a stageId to a map of its stageData attempts
 */
case class RawSparkApplicationAttempt(
    appAttemptInfo: ApplicationAttemptInfo,
    appEnvInfo: ApplicationEnvironmentInfo,
    jobDataMap: JobDataMap,
    stageDataMap: StageDataMap)

object RawSparkApplicationAttempt {
  type JobId = Int
  type JobDataMap = Map[JobId, JobData]
  type AttemptId = Int
  type StageId = Int
  type StageDataMap = Map[StageId, Map[AttemptId, StageData]]
}
