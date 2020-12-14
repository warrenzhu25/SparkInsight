package com.microsoft.spark.insight.utils.spark

/**
 * An interface to be implemented by Spark Metric providers, e.g. SHS and Presto
 */
trait SparkMetricsRetriever {

  /**
   * Retrieves the first application attempt info for all of the applicationIds provided, and wraps them in a
   * [[RawSparkMetrics]]
   *
   * @param appIds ApplicationIds to fetch
   * @return A sequence of wrapped metrics
   */
  def retrieve(appIds: Seq[String]): Seq[RawSparkMetrics]
}
