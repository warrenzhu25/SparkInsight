package com.microsoft.spark.insight.cli.argument

import com.microsoft.spark.insight.utils.spark.SparkMetricsRetriever

/**
 * A trait for classes that produce a report as a [[String]]
 */
private[cli] trait ReportGeneration {

  /**
   * Produce a report as a [[String]]
   *
   * @param sparkMetricsRetriever a Spark Metrics Retriever for fetching metrics
   * @return string representation of a report
   */
  def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String
}
