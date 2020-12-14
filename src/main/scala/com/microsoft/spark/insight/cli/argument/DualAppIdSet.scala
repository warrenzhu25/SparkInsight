package com.microsoft.spark.insight.cli.argument

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Trait for subcommands with two sets of appIds input
 */
private[argument] trait DualAppIdSet { _: ScallopConf =>
  val appIds1: ScallopOption[List[String]] = opt[List[String]](
    name = "appIds1",
    required = true,
    validate = list => list.forall(_.startsWith("application_")) && list.nonEmpty,
    short = 'a',
    argName = "appIds1",
    descr = "The first set of applicationIds to query")

  val appIds2: ScallopOption[List[String]] = opt[List[String]](
    name = "appIds2",
    required = true,
    validate = list => list.forall(_.startsWith("application_")) && list.nonEmpty,
    short = 'b',
    argName = "appIds2",
    descr = "The second set of applicationIds to query")
}
