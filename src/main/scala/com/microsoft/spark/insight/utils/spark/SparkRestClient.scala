package com.microsoft.spark.insight.utils.spark

import java.net.URI
import java.util.Date

import javax.ws.rs.client.{ClientBuilder, WebTarget}
import javax.ws.rs.core.MediaType
import org.apache.spark.status.api.v1._
import org.glassfish.jersey.client.ClientProperties
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

/**
 * A REST client used to fetch Application data from a Spark History Server
 *
 * @param historyServerUri address of the history server to be used
 */
class SparkRestClient(historyServerUri: URI) {
  val LOGGER: Logger = LoggerFactory.getLogger(classOf[SparkRestClient])
  import SparkRestClient._

  private val client = ClientBuilder.newClient()

  private val apiTarget: WebTarget = client
    .property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
    .property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT)
    .target(historyServerUri)
    .path(API_V1_MOUNT_PATH)

  /**
   * Fetch [[ApplicationInfo]]s within the specified date range and that match the specified status
   *
   * @param maxApps limit the number of fetched apps
   * @param minStartDate earliest start date to fetch
   * @param maxStartDate latest start date to fetch
   * @param status only fetch applications with this status
   * @return ApplicationInfos
   */
  def getApplicationInfos(
      maxApps: Integer,
      minStartDate: Date,
      maxStartDate: Date,
      status: ApplicationStatus): Seq[ApplicationInfo] = {
    val appTarget = apiTarget
      .path(s"applications")
      .queryParam("limit", maxApps)
      .queryParam("minDate", SparkJsonUtils.DATE_FORMAT.format(minStartDate))
      .queryParam("maxDate", SparkJsonUtils.DATE_FORMAT.format(maxStartDate))
      .queryParam("status", status)
    get(appTarget, SparkJsonUtils.fromJson[Seq[ApplicationInfo]])
  }

  /**
   * Fetch a single [[ApplicationInfo]] for the specified appId
   *
   * @param appId ApplicationId to fetch
   * @return ApplicationInfo
   */
  def getApplicationInfo(appId: String): ApplicationInfo = {
    val appTarget = apiTarget.path(s"applications/${appId}")
    get(appTarget, SparkJsonUtils.fromJson[ApplicationInfo])
  }

  /**
   * Fetch [[ApplicationEnvironmentInfo]] for the specified applicationId and applicationAttemptId
   *
   * @param appId applicationId to fetch
   * @param appAttemptId application AttemptId to fetch
   * @return ApplicationEnvironmentInfo
   */
  def getApplicationEnvironmentInfo(appId: String, appAttemptId: String): ApplicationEnvironmentInfo = {
    val appEnvTarget = apiTarget.path(s"applications/${appId}/${appAttemptId}/environment")
    get(appEnvTarget, SparkJsonUtils.fromJson[ApplicationEnvironmentInfo])
  }

  /**
   * Fetch Job information for the specified applicationId and applicationAttemptId
   *
   * @param appId applicationId to fetch
   * @param appAttemptId application AttemptId to fetch
   * @return JobDatas
   */
  def getJobDatas(appId: String, appAttemptId: String): Seq[JobData] = {
    val jobsTarget = apiTarget.path(s"applications/${appId}/${appAttemptId}/jobs")
    get(jobsTarget, SparkJsonUtils.fromJson[Seq[JobData]])
  }

  /**
   * Fetch all of the Stages information for the specified applicationId and applicationAttemptId
   *
   * @param appId applicationId to fetch
   * @param appAttemptId application AttemptId to fetch
   * @return StageDatas
   */
  def getStagesWithSummaries(appId: String, appAttemptId: String): Seq[StageData] = {
    val stageTarget = apiTarget.path(s"applications/${appId}/${appAttemptId}/stages/withSummaries")
    get(stageTarget, SparkJsonUtils.fromJson[Seq[StageData]])
  }

  /**
   * Fetch Stage information for the specified applicationId and applicationAttemptId
   *
   * @param appId applicationId to fetch
   * @param appAttemptId application AttemptId to fetch
   * @param stageId stageId to fetch
   * @return StageDatas
   */
  def getStageAttemptDatas(appId: String, appAttemptId: String, stageId: String): Seq[StageData] = {
    val stageTarget = apiTarget.path(s"applications/${appId}/${appAttemptId}/stages/${stageId}")
    get(stageTarget, SparkJsonUtils.fromJson[Seq[StageData]])
  }

  protected def get[T](webTarget: WebTarget, converter: String => T): T = {
    LOGGER.info("calling REST API at {}", webTarget.getUri)
    try {
      converter(webTarget.request(MediaType.APPLICATION_JSON).get(classOf[String]))
    } catch {
      case NonFatal(e) =>
        LOGGER.error("Error reading from URI: {}. Exception Message = {}", webTarget.getUri, e.getMessage, e)
        throw e
    }
  }
}

object SparkRestClient {
  val API_V1_MOUNT_PATH = "api/v1"
  val CONNECTION_TIMEOUT = 5000
  val READ_TIMEOUT = 240000
}
