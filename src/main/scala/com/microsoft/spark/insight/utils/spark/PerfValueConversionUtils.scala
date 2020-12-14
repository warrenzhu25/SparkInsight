package com.microsoft.spark.insight.utils.spark

import java.util.concurrent.TimeUnit

import com.microsoft.spark.insight.utils.PerfMetric
import com.microsoft.spark.insight.utils.PerfMetric._

import scala.concurrent.duration.{Duration, MILLISECONDS}

/**
 * Utils object to convert PerfValue into corresponding units
 */
object PerfValueConversionUtils {

  /**
   * For a given PerfMetric, extract the units to convert the perfValue into, and the corresponding transformer
   * @param perfMetric a PerfMetric
   * @param decimalDigits precision required
   * @return corresponding units and transformer
   */
  def getUnitsAndTransformer(perfMetric: PerfMetric, decimalDigits: Int): (String, Double => String) = {
    perfMetric match {
      case perfMetric if MILLIS_TO_MINUTES_PERF_METRICS.contains(perfMetric) =>
        ("minutes", millisToMinutes(_, decimalDigits))
      case perfMetric if BYTES_TO_GB_PERF_METRICS.contains(perfMetric) => ("GB", bytesToGBs(_, decimalDigits))
      case perfMetric if TO_THOUSANDS_PERF_METRICS.contains(perfMetric) => ("thousands", toThousands(_, decimalDigits))
    }
  }

  /**
   * convert value from milli seconds to minutes
   * @param millis - value in milli seconds
   * @return string representation of value in minutes
   */
  def millisToMinutes(millis: Double, decimalDigits: Int): String =
    roundUpDouble(Duration(millis, MILLISECONDS).toUnit(TimeUnit.MINUTES), decimalDigits)

  /**
   * convert value from bytes to Giga Bytes
   * @param bytes - value in bytes
   * @return string representation of value in Giga Bytes
   */
  def bytesToGBs(bytes: Double, decimalDigits: Int): String =
    roundUpDouble(bytes / scala.math.pow(2, 30), decimalDigits)

  /**
   * convert value to thousands
   * @param num number to convert to thousands
   * @return string representation of value in thousands
   */
  def toThousands(num: Double, decimalDigits: Int): String = roundUpDouble(num / 1000, decimalDigits)

  /**
   * A convenience method to convert a [[Double]] to a string, while rounding up to the nearest number with num of digit
   * precision. If the absolute value is equal or larger than 100, it will not include any decimal places.
   *
   * @param num double to convert to string
   * @param decimalDigits number of decimal digits in string
   * @return string representation of the input
   */
  def roundUpDouble(num: Double, decimalDigits: Int): String = scala.math.abs(num) match {
    case absNum if absNum < scala.math.pow(10, -decimalDigits) * 0.5 => "0"
    case absNum if absNum >= 100 => "%.0f".format(num)
    case _ => s"%.${decimalDigits}f".format(num)
  }

  /**
   * List of PerfMetrics whose raw values are to be converted from milli seconds to minutes.
   */
  val MILLIS_TO_MINUTES_PERF_METRICS = List(
    TOTAL_RUNNING_TIME,
    SUBMISSION_TO_FIRST_LAUNCH_DELAY,
    FIRST_LAUNCH_TO_COMPLETED,
    EXECUTOR_RUNTIME_LESS_SHUFFLE,
    EXECUTOR_RUNTIME,
    EXECUTOR_CPU_TIME,
    JVM_GC_TIME,
    SHUFFLE_READ_FETCH_WAIT_TIME,
    SHUFFLE_WRITE_TIME,
    NET_IO_TIME)

  /**
   * List of PerfMetrics whose raw values are to be converted from Bytes to Giga Bytes.
   */
  val BYTES_TO_GB_PERF_METRICS =
    List(INPUT_SIZE, OUTPUT_SIZE, SHUFFLE_READ_SIZE, SHUFFLE_WRITE_SIZE, MEM_SPILLS, DISK_SPILLS)

  /**
   * List of PerfMetrics whose raw values are to be converted to thousands.
   */
  val TO_THOUSANDS_PERF_METRICS = List(INPUT_RECORDS, OUTPUT_RECORDS, SHUFFLE_READ_RECORDS, SHUFFLE_WRITE_RECORDS)
}
