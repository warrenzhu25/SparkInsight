package com.microsoft.spark.insight.cli

import com.microsoft.spark.insight.utils.spark.SparkMetricsRetriever
import javax.ws.rs.NotFoundException

/**
 * An interface to be implemented by varieties of Spark Perf analysis report for Text version of the Report.
 *
 * @tparam T the type of Spark id collection
 */
trait TextReport[T <: AppIdSets] {

  /**
   * Generate Text version of Spark performance analysis report
   *
   * @param appIdSets a collection of Spark appId strings
   * @param sparkMetricsRetriever a metrics retriever instance for fetching metrics
   * @return performance report String
   */
  def genTextReport(appIdSets: T)(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
    try {
      genTextReportBody(AppReportSets(appIdSets))
    } catch {
      case nfe: NotFoundException => throw new IllegalArgumentException(s"$appIdSets cannot be found.", nfe)
      case e: Exception => throw e
    }
  }

  /**
   * Define the report details
   *
   * @param appReportSets a collection of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return performance report String
   */
  protected def genTextReportBody[U <: AppReportSets[T]](appReportSets: U): String
}
