package com.microsoft.spark.insight.utils.spark

import java.net.URI

import scala.collection.immutable.ListMap
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * A Spark History Server based metrics retriever
 *
 * Will connect to a Spark History Server to retrieve application info using a [[SparkRestClient]]
 */
class SparkHistoryServerMetricsRetriever(historyServerUri: URI) extends SparkMetricsRetriever {
  private val client = new SparkRestClient(historyServerUri)

  // The default scala setting is 1 thread per core; We set a larger number as the parallel processing
  // mainly waits for the REST response
  private val folkJoinTaskSupport = new ForkJoinTaskSupport(
    new ForkJoinPool(SparkHistoryServerMetricsRetriever.TARGET_THREAD_NUM))

  /**
   * Retrieves the first application attempt info for all of the applicationIds provided, and wraps them in a
   * [[RawSparkMetrics]]
   *
   * @param appIds ApplicationIds to fetch
   * @return A sequence of wrapped metrics
   */
  override def retrieve(appIds: Seq[String]): Seq[RawSparkMetrics] = {
    val parAppIds = appIds.par

    parAppIds.tasksupport = folkJoinTaskSupport
    parAppIds.map(buildRawSparkMetrics).seq
  }

  private def buildRawSparkMetrics(appId: String): RawSparkMetrics = {
    val appInfo = client.getApplicationInfo(appId)
    val attemptMap = appInfo.attempts
      .filter(_.attemptId.isDefined)
      .map(appAttempt => {
        val appAttemptId = appAttempt.attemptId.getOrElse(
          throw new IllegalStateException(
            "Undefined attemptId found - should never be thrown since undefined attempts were already filtered"))
        val envInfo = client.getApplicationEnvironmentInfo(appId, appAttemptId)

        // Explicitly use a ListMap to preserve ordering - simplifies testing
        val jobDataMap = ListMap(client.getJobDatas(appId, appAttemptId).map(jobData => jobData.jobId -> jobData): _*)

        val stageDataMap = client
          .getStagesWithSummaries(appId, appAttemptId)
          .groupBy(_.stageId)
          .mapValues(stageDatas => stageDatas.map(stageData => stageData.attemptId -> stageData).toMap)

        appAttemptId -> RawSparkApplicationAttempt(appAttempt, envInfo, jobDataMap, stageDataMap)
      })
      .toMap

    RawSparkMetrics(appInfo, attemptMap)
  }
}

/**
 * Companion object of SparkHistoryServerMetricsRetriever
 */
object SparkHistoryServerMetricsRetriever {

  /**
   * Setting number of threads for parallel collection processing
   */
  private val TARGET_THREAD_NUM: Int = 16
}
