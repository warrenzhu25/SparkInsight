package com.microsoft.spark.insight.cli.argument

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Trait for hiding the "-h" or "--help" options from subcommands
 */
private[argument] trait HiddenHelp { _: ScallopConf =>
  val help: ScallopOption[Boolean] = opt[Boolean](hidden = true)
}
