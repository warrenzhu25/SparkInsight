package com.microsoft.spark.insight.cli

/**
 * The script to generate resource report given an application id.
 */
object SparkResourceReportCli extends TextReport[AppIdSet] {
  // The specified format string to print report
  val PRINT_FORMAT = "%-38s %18s %15s %16s %20s\n"

  /**
   * Generate a resource report given a set of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   *
   * @param appReportSet a set of [[com.microsoft.spark.insight.utils.spark.SparkAppPerfReport]]s
   * @return report string
   */
  override protected def genTextReportBody[U <: AppReportSets[AppIdSet]](appReportSet: U): String = {
    val sb = new StringBuilder()

    sb.append(
      String.format("\n" + PRINT_FORMAT, "appId", "executorMemory (GB)", "cores", "cpuTime (hrs)", "resource (GB-hrs)"))
    sb.append(String.format("-" * 112 + "\n"))

    appReportSet.appReports.foreach(report => {
      val cpuHour = report.getCpuDuration.toHours
      val sparkProps = report.getSparkProps

      val executorMem = GridBenchCliUtil.strToGB(sparkProps("spark.executor.memory"))
      val executorMemOverhead = GridBenchCliUtil
        .getMemoryOverheadInGB(executorMem, sparkProps.getOrElse("spark.yarn.executor.memoryOverhead", " "))
      val executorCores = sparkProps("spark.executor.cores").toInt
      val wholeMemory = executorMem + executorMemOverhead
      val resource = cpuHour * wholeMemory / executorCores
      val appID = report.rawSparkMetrics.appId

      sb.append(String
        .format(PRINT_FORMAT, appID, wholeMemory.toString, executorCores.toString, cpuHour.toString, resource.toString))
    })
    sb.append(String.format("-" * 112))
    sb.toString
  }
}
