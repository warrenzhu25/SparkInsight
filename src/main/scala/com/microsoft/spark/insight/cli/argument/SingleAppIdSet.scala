package com.microsoft.spark.insight.cli.argument

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Trait for subcommands with one set of appIds input
 */
private[argument] trait SingleAppIdSet { _: ScallopConf =>
  val appIds: ScallopOption[List[String]] = opt[List[String]](
    name = "appIds",
    required = true,
    validate = list => list.forall(_.startsWith("application_")) && list.nonEmpty,
    argName = "appIds",
    descr = "application Id list to query")
}
