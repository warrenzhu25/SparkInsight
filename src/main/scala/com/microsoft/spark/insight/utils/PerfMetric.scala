package com.microsoft.spark.insight.utils

/**
 * PerfMetric case class
 *
 * @param name metric name
 * @param description metric description
 * @param useInRegressionReport whether this metric should be analyzed for regression or not. Some metrics collect data
 *                              that when changed, does not necessarily imply an improvement or degradation in
 *                              performance. For example, if an app ran on a larger input volume, that may not
 *                              constitute as a problem. However, reporting this change is valuable when comparing runs
 *                              as it may reflect in other metrics.
 * @param accumulable whether this metric should be summed-up and added to the summarized view of an app. Some metrics
 *                    may misrepresent an overview of an app when accumulated - for example, if such metrics may count
 *                    time periods which may occur in parallel.
 */
case class PerfMetric(
    name: String,
    displayName: String,
    description: String,
    useInRegressionReport: Boolean = true,
    accumulable: Boolean = true) {
  override def toString = s"$name"
}

/**
 * Define common PerfMetric constants.
 */
object PerfMetric {
  val TOTAL_RUNNING_TIME: PerfMetric =
    PerfMetric("totalRuntimeMs", "Total Runtime", "Total elapsed running time", accumulable = false)
  val SUBMISSION_TO_FIRST_LAUNCH_DELAY: PerfMetric = PerfMetric(
    "submissionToFirstLaunchDelayMs",
    "Submission to Launch Delay",
    "Delay between stage submission and launching of its first task",
    accumulable = false)
  val FIRST_LAUNCH_TO_COMPLETED: PerfMetric = PerfMetric(
    "firstLaunchToCompletedMs",
    "First Launch till Completed",
    "Duration between launching the first task and stage completion",
    accumulable = false)
  val EXECUTOR_RUNTIME_LESS_SHUFFLE: PerfMetric = PerfMetric(
    "executorRunTimeLessShuffleMs",
    "Executor Runtime w/o Shuffle",
    "Executor run time excluding shuffle time")
  val EXECUTOR_RUNTIME: PerfMetric =
    PerfMetric("executorRunTimeMs", "Executor Runtime", "Total elapsed time spent by the executor running tasks")
  val EXECUTOR_CPU_TIME: PerfMetric = PerfMetric(
    "executorCpuTimeMs",
    "Executor CPU Time",
    "Total active CPU time spent by the executor running the main task thread")
  val JVM_GC_TIME: PerfMetric = PerfMetric("jvmGcTimeMs", "JVM GC Time", "Total time JVM spent in garbage collection")
  val SHUFFLE_READ_FETCH_WAIT_TIME: PerfMetric = PerfMetric(
    "shuffleReadFetchWaitTimeMs",
    "Shuffle Read Wait Time",
    "Total time during which tasks were blocked waiting for remote shuffle data")
  val SHUFFLE_WRITE_TIME: PerfMetric =
    PerfMetric("shuffleWriteTimeMs", "Shuffle Write Time", "Total Shuffle write time spent by tasks")
  val INPUT_SIZE: PerfMetric =
    PerfMetric("inputSizeBytes", "Input Size", "Total input data consumed by tasks", useInRegressionReport = false)
  val INPUT_RECORDS: PerfMetric = PerfMetric(
    "inputRecords",
    "Input Records",
    "Total number of records consumed by tasks",
    useInRegressionReport = false)
  val OUTPUT_SIZE: PerfMetric =
    PerfMetric("outputSizeBytes", "Output Size", "Total output data produced by tasks", useInRegressionReport = false)
  val OUTPUT_RECORDS: PerfMetric = PerfMetric(
    "outputRecords",
    "Output Records",
    "Total number of records produced by tasks",
    useInRegressionReport = false)
  val SHUFFLE_READ_SIZE: PerfMetric = PerfMetric(
    "shuffleReadSizeBytes",
    "Shuffle Read Size",
    "Total shuffle data consumed by tasks",
    useInRegressionReport = false)
  val SHUFFLE_READ_RECORDS: PerfMetric = PerfMetric(
    "shuffleReadRecords",
    "Shuffle Read Records",
    "Total number of shuffle records consumed by tasks",
    useInRegressionReport = false)
  val SHUFFLE_WRITE_SIZE: PerfMetric = PerfMetric(
    "shuffleWriteSizeBytes",
    "Shuffle Write Size",
    "Total shuffle data produced by tasks",
    useInRegressionReport = false)
  val SHUFFLE_WRITE_RECORDS: PerfMetric = PerfMetric(
    "shuffleWriteRecords",
    "Shuffle Write Records",
    "Total number of shuffle records produced by tasks",
    useInRegressionReport = false)
  val MEM_SPILLS: PerfMetric = PerfMetric("memSpillsBytes", "Memory Spill Size", "Total data spilled to memory")
  val DISK_SPILLS: PerfMetric = PerfMetric("diskSpillsBytes", "Disk Spill Size", "Total data spilled to disk")
  val NET_IO_TIME: PerfMetric = PerfMetric("netIoTimeMs", "Net I/O Time", "Total time spent accessing external storage")
}
