package com.microsoft.spark.insight.cli.argument

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * A trait for classes that determine whether to show per-stage metrics
 */
private[argument] trait ShowStages { _: ScallopConf =>

  /**
   * Determines whether to show per-stage metrics
   *
   * @return true or false to show per-stages metrics
   */
  val showStages: ScallopOption[Boolean] =
    opt[Boolean](
      name = "All",
      required = false,
      default = Some(false),
      descr = "(Optional) Show all per-stage metrics in addition to the summary")
}
