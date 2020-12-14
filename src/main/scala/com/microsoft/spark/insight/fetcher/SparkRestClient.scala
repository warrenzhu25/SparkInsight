/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.microsoft.spark.insight.fetcher

import java.io.BufferedInputStream
import java.net.URI
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.zip.ZipInputStream
import java.util.{Calendar, SimpleTimeZone}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.microsoft.spark.insight.fetcher.status._
import javax.ws.rs.client.{Client, ClientBuilder, WebTarget}
import javax.ws.rs.core.MediaType
import org.apache.log4j.Logger
import org.glassfish.jersey.client.ClientProperties

import scala.concurrent.duration.{Duration, HOURS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.control.NonFatal

/**
 * A client for getting data from the Spark monitoring REST API,
 * e.g. <https://spark.apache.org/docs/1.4.1/monitoring.views.html#rest-api>.
 *
 * Jersey classloading seems to be brittle (at least when testing in the console),
 * so some of the implementation is non-lazy
 * or synchronous when needed.
 */
class SparkRestClient {

  import SparkRestClient._

  private val logger: Logger = Logger.getLogger(classOf[SparkRestClient])

  private val client: Client = ClientBuilder.newClient()

  def fetchData(trackingUrl: String)(
    implicit ec: ExecutionContext
  ): Future[SparkApplicationData] = {
    val (historyServerUri, appId) = spilt(trackingUrl)

    Future {
      val localData = readDataLocally(appId)

      if (localData.nonEmpty) {
        localData.get
      } else {
        val (applicationInfo, env, attemptTarget) = getApplicationMetaData(appId, historyServerUri)

        val futureJobDatas = Future {
          getJobDatas(attemptTarget)
        }
        val futureStageDatas = Future {
          getStageDatas(attemptTarget)
        }
        val futureExecutorSummaries = Future {
          getExecutorSummaries(attemptTarget)
        }
        val futureTasks = Future {
          getTasksOfFailedStages(attemptTarget)
        }

        val appData = SparkApplicationData(
          appId,
          env.sparkProperties.map(p => p._1 -> p._2).toMap,
          applicationInfo,
          Await.result(futureJobDatas, DEFAULT_TIMEOUT),
          Await.result(futureStageDatas, DEFAULT_TIMEOUT),
          Await.result(futureExecutorSummaries, DEFAULT_TIMEOUT),
          Await.result(futureTasks, DEFAULT_TIMEOUT)
        )

        writeDataLocally(appData)
        appData
      }
    }
  }

  private def readDataLocally(appId: String): Option[SparkApplicationData] = {
    val path = s"""D:\\SparkInsight\\$appId.json"""

    if (!Files.exists(Paths.get(path))) {
      return None
    }

    val bufferedSource = Source.fromFile(path)

    try {
      logger.info(s"Reading data from $path")
      Some(SparkRestObjectMapper.readValue(bufferedSource.mkString, classOf[SparkApplicationData]))
    } finally {
      bufferedSource.close()
    }
  }

  private def writeDataLocally(sparkApplicationData: SparkApplicationData) = {
    val file = s"""D:\\SparkInsight\\${sparkApplicationData.appId}.json"""
    val path = Paths.get(file)
    if (!Files.exists(path)) {
      logger.info(s"Writing data from $file")
      Files.createDirectories(path.getParent)
      Files.createFile(path)
      Files.write(path, SparkRestObjectMapper.writeValueAsBytes(sparkApplicationData))
    }
  }

  private def spilt(trackingUrl: String): (String, String) = {
    val uri = new URI(trackingUrl)
    val host = s"${uri.getScheme}://${uri.getHost}:${uri.getPort}"
    val appId = uri.getPath.split('/').find(_.startsWith("application_")).getOrElse("")
    (host, appId)
  }

  private def getApiTarget(historyServerUri: String): WebTarget =
    client
      .property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
      .property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT)
      .target(historyServerUri)
      .path(API_V1_MOUNT_PATH)

  private def getApplicationMetaData(appId: String, historyServerUri: String):
  (ApplicationInfoImpl, ApplicationEnvironmentInfo, WebTarget) = {
    val apiTarget = getApiTarget(historyServerUri)
    val appTarget = apiTarget.path(s"applications/${appId}")
    logger.info(s"calling REST API at ${appTarget.getUri}")

    val applicationInfo = getApplicationInfo(appTarget)

    val lastAttemptId = applicationInfo.attempts.maxBy {
      _.startTime
    }.attemptId
    val attemptTarget = lastAttemptId.map(appTarget.path).getOrElse(appTarget)
    val applicationEnvironmentInfo = getEnv(attemptTarget)
    (applicationInfo, applicationEnvironmentInfo, attemptTarget)
  }

  private def getApplicationInfo(appTarget: WebTarget): ApplicationInfoImpl = {
    get(appTarget, SparkRestObjectMapper.readValue[ApplicationInfoImpl])
  }

  private[fetcher] def getApplicationLogs(logTarget: WebTarget): ZipInputStream = {
    try {
      val is = logTarget.request(MediaType.APPLICATION_OCTET_STREAM)
        .get(classOf[java.io.InputStream])
      new ZipInputStream(new BufferedInputStream(is))
    } catch {
      case NonFatal(e) => {
        logger.error(s"error reading logs ${logTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }

  private def getJobDatas(attemptTarget: WebTarget): Seq[JobDataImpl] = {
    val target = attemptTarget.path("jobs")
    get(target, SparkRestObjectMapper.readValue[Seq[JobDataImpl]])
  }

  private def getStageDatas(attemptTarget: WebTarget): Seq[StageDataImpl] = {
    val target = attemptTarget.path("stages")
    get(target, SparkRestObjectMapper.readValue[Seq[StageDataImpl]])
  }

  private def getExecutorSummaries(attemptTarget: WebTarget): Seq[ExecutorSummaryImpl] = {
    val target = attemptTarget.path("allexecutors")
    get(target, SparkRestObjectMapper.readValue[Seq[ExecutorSummaryImpl]])
  }

  private def getTasksOfFailedStages(attemptTarget: WebTarget): Map[String, Seq[TaskDataImpl]] = {
    val stageDatas = getTasks(attemptTarget)
    stageDatas.map(s => s"${s.stageId}-${s.attemptId}" -> s.tasks.values.toSeq).toMap
  }

  private def getTasks(attemptTarget: WebTarget): Seq[StageData] = {
    val target = attemptTarget.path(s"allTasks")
                              .queryParam("status", "failed")
    get(target, SparkRestObjectMapper.readValue[Seq[StageDataImpl]])
  }

  private def getEnv(attemptTarget: WebTarget): ApplicationEnvironmentInfo = {
    val target = attemptTarget.path("environment")
    get(target, SparkRestObjectMapper.readValue[ApplicationEnvironmentInfo])
  }
}

object SparkRestClient {
  val API_V1_MOUNT_PATH = "api/v1"
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_TIMEOUT = Duration(1, HOURS);
  val CONNECTION_TIMEOUT = 60000
  val READ_TIMEOUT = 60000
  val logger: Logger = Logger.getLogger(SparkRestClient.getClass)

  val SparkRestObjectMapper = {
    val dateFormat = {
      val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
      val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
      iso8601.setCalendar(cal)
      iso8601
    }

    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setDateFormat(dateFormat)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }

  def get[T](webTarget: WebTarget, converter: String => T): T =
    try {
      converter(webTarget.request(MediaType.APPLICATION_JSON).get(classOf[String]))
    } catch {
      case NonFatal(e) => {
        logger.error(s"error fetching ${webTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
}
