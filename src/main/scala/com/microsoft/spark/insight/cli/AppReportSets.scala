package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.utils.spark.{SparkAppPerfReport, SparkMetricsRetriever}

/**
 * Base trait to specify a collection of [[SparkAppPerfReport]]s
 * The concrete sub-types are used for propagating fetched reports in [[ReportCli]] to its sub-types
 *
 * @tparam T a concrete sub-type of [[AppIdSets]] that corresponds to this sub-type of AppReportSets
 */
sealed trait AppReportSets[T <: AppIdSets] {

  /**
   * The corresponding [[AppIdSets]] instance to this report set
   */
  protected val appIdSets: T

  override def toString: String = appIdSets.toString
}

object AppReportSets {
  /*
   * A set of implicit conversions from AppReportSets[T <: AppIdSets] to concrete sub-types of AppReportSets.
   * The implicit conversion assumes that there will be no ambiguity between mapping a AppReportSets[T] to a sub-type.
   */
  implicit def appReportSetsToSingleAppReport(appReportSets: AppReportSets[AppId]): SingleAppReport =
    appReportSets.asInstanceOf[SingleAppReport]
  implicit def appReportSetsToAppReportSet(appReportSets: AppReportSets[AppIdSet]): AppReportSet =
    appReportSets.asInstanceOf[AppReportSet]
  implicit def appReportSetsToAppReportPair(appReportSets: AppReportSets[AppIdPair]): AppReportPair =
    appReportSets.asInstanceOf[AppReportPair]
  implicit def appReportSetsToAppReportSetPair(appReportSets: AppReportSets[AppIdSetPair]): AppReportSetPair =
    appReportSets.asInstanceOf[AppReportSetPair]

  /**
   * Create an AppReportSets from the given [[AppIdSets]]
   *
   * @param appIdSets appId collection to retrieve metrics for
   * @param sparkMetricsRetriever metrics retriever for fetching metrics
   * @tparam T a sub-type of AppIdSets
   * @return an instance of AppReportSets
   */
  def apply[T <: AppIdSets](appIdSets: T)(implicit sparkMetricsRetriever: SparkMetricsRetriever): AppReportSets[T] = {
    def retrieve(appIds: Seq[String]) = sparkMetricsRetriever.retrieve(appIds).map(new SparkAppPerfReport(_))

    // Match all sub-types of AppIdSets and convert to the appropriate sub-type of AppReportSets
    (appIdSets match {
      case appId: AppId => SingleAppReport(appId, retrieve(Seq(appId.appId)).head)
      case appIdSet: AppIdSet => AppReportSet(appIdSet, retrieve(appIdSet.appIds))
      case appIdPair: AppIdPair =>
        AppReportPair(appIdPair, retrieve(Seq(appIdPair.appId1)).head, retrieve(Seq(appIdPair.appId2)).head)
      case appIdSetPair: AppIdSetPair =>
        AppReportSetPair(appIdSetPair, retrieve(appIdSetPair.appIds1), retrieve(appIdSetPair.appIds2))
    }).asInstanceOf[AppReportSets[T]]
  }
}

/**
 * A single [[SparkAppPerfReport]]
 *
 * @param appIdSets an [[AppId]]
 * @param appReport Spark App Perf Report
 */
private[cli] case class SingleAppReport private (appIdSets: AppId, appReport: SparkAppPerfReport)
    extends AppReportSets[AppId]

/**
 * A single set of [[SparkAppPerfReport]]s
 *
 * @param appIdSets an [[AppIdSet]]
 * @param appReports one set of Spark App Perf Reports
 */
private[cli] case class AppReportSet private (appIdSets: AppIdSet, appReports: Seq[SparkAppPerfReport])
    extends AppReportSets[AppIdSet]

/**
 * One pair of [[SparkAppPerfReport]]s
 *
 * @param appIdSets an [[AppIdPair]]
 * @param appReport1 the first Spark App Perf Report
 * @param appReport2 the second Spark App Perf Report
 */
private[cli] case class AppReportPair private (
    appIdSets: AppIdPair,
    appReport1: SparkAppPerfReport,
    appReport2: SparkAppPerfReport)
    extends AppReportSets[AppIdPair]

/**
 * Two sets of [[SparkAppPerfReport]]s
 *
 * @param appIdSets an [[AppIdSetPair]]
 * @param appReports1 the first set of Spark App Perf Reports
 * @param appReports2 the second set of Spark App Perf Reports
 */
private[cli] case class AppReportSetPair private (
    appIdSets: AppIdSetPair,
    appReports1: Seq[SparkAppPerfReport],
    appReports2: Seq[SparkAppPerfReport])
    extends AppReportSets[AppIdSetPair]
