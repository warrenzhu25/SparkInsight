package com.microsoft.spark.insight.cli.argument

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * A trait that defines if Html file is to be generated.
 */
private[argument] trait GenerateHtml { _: ScallopConf =>

  /**
   * Boolean value to specify Html generation.
   */
  val generateHtml: ScallopOption[Boolean] = opt[Boolean](
    name = "views/html",
    short = 'H',
    required = false,
    default = Some(false),
    descr = "(Optional) Generate an interactive HTML version of the report")
}
