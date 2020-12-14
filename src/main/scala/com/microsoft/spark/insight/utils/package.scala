package com.microsoft.spark.insight

/**
 * Common utilities
 */
package object utils {
  type PerfValue = Long
  type PerfMetrics[T] = Map[PerfMetric, T]
}
