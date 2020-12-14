package com.microsoft.spark.insight.cli.argument

import com.microsoft.spark.insight.cli._
import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.utils.spark.SparkMetricsRetriever
import org.rogach.scallop.exceptions.{Help, ScallopException, ScallopResult}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, Subcommand, ValueConverter, throwError}

/**
 * [[ArgumentConf]] companion object
 */
private[cli] object ArgumentConf {
  def apply(arguments: Seq[String]) = new ArgumentConf(arguments)
}

/**
 * Use [[org.rogach.scallop.Scallop]] to manage/validate user inputs.
 *
 * @param arguments inputs from the user
 */
private[cli] class ArgumentConf(arguments: Seq[String]) extends ScallopConf(arguments) with HiddenHelp {
  abstract class GridBenchSubcommand(commandNameAndAliases: String*)
      extends Subcommand(commandNameAndAliases: _*)
      with ReportGeneration
      with HiddenHelp

  abstract class PerfReportSubcommand(commandNameAndAliases: String*)
      extends GridBenchSubcommand(commandNameAndAliases: _*)
      with ShowStages

  // shs endpoint parameter definition
  val shsEndpoint: ScallopOption[String] = opt[String](
    name = "shs",
    required = false,
    default = Some("holdem"),
    argName = "shs_endpoint",
    descr =
      "Spark History Server Endpoint to use - either a full URL or a Grid cluster name, e.g. 'war'. 'holdem' will be " +
        "assumed if no value was provided.")

  // Print out the version info
  val version: ScallopOption[Boolean] = opt[Boolean](name = "version", required = false, descr = "Version info")

  private val resourceCmd: GridBenchSubcommand = new GridBenchSubcommand("resource") with SingleAppIdSet {
    descr("Get spark application resource report")
    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String =
      SparkResourceReportCli.genTextReport(AppIdSet(appIds()))
  }

  private val singleReportCmd: GridBenchSubcommand = new PerfReportSubcommand("report") with GenerateHtml {
    descr("Performance analysis for a single Spark application run")

    private val appId: ScallopOption[String] = opt[String](
      name = "appId",
      required = true,
      validate = _.startsWith("application_"),
      argName = "appId",
      descr = "application Id to query")

    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
      if (generateHtml()) {
        SparkAppPerfReportCli(showStages()).genHtmlReport(AppId(appId()))
      } else {
        SparkAppPerfReportCli(showStages()).genTextReport(AppId(appId()))
      }
    }
  }

  private val diffReportCmd: GridBenchSubcommand = new PerfReportSubcommand("diff") with GenerateHtml {
    descr("Comparative performance analysis for two individual Spark application runs")

    private val pairConverter = new ValueConverter[AppIdPair] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[AppIdPair]] =
        s match {
          case (_, appId1 :: appId2 :: Nil) :: Nil => Right(Some(AppIdPair(appId1, appId2)))
          case Nil => Right(None) // no AppIdPair found
          case _ => Left("The input arguments need to be a pair of appIds.")
        }

      override val argType: ArgType.V = org.rogach.scallop.ArgType.LIST

      override def argFormat(name: String): String = "<appId1> <appId2>"
    }

    private val appIdPair: ScallopOption[AppIdPair] =
      opt[AppIdPair](name = "appIds", required = true, short = 'a', descr = "two application Ids to query")(
        pairConverter)

    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
      if (generateHtml()) {
        SparkAppPerfDiffReportCli(showStages()).genHtmlReport(appIdPair())
      } else {
        SparkAppPerfDiffReportCli(showStages()).genTextReport(appIdPair())
      }
    }
  }

  private val aggReportCmd: GridBenchSubcommand = new PerfReportSubcommand("agg") with SingleAppIdSet
  with GenerateHtml {
    descr("Aggregated Performance analysis for multiple Spark application runs")
    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
      if (generateHtml()) {
        SparkAppPerfAggregateReportCli(showStages()).genHtmlReport(AppIdSet(appIds()))
      } else {
        SparkAppPerfAggregateReportCli(showStages()).genTextReport(AppIdSet(appIds()))
      }
    }
  }

  private val aggDiffReportCmd: GridBenchSubcommand = new PerfReportSubcommand("aggdiff") with DualAppIdSet
  with GenerateHtml {
    descr("Comparative performance analysis for two sets of Spark application runs")

    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
      if (generateHtml()) {
        SparkAppPerfAggregateDiffReportCli(showStages()).genHtmlReport(AppIdSetPair(appIds1(), appIds2()))
      } else {
        SparkAppPerfAggregateDiffReportCli(showStages()).genTextReport(AppIdSetPair(appIds1(), appIds2()))
      }
    }
  }

  private val regressionReportCmd: GridBenchSubcommand = new PerfReportSubcommand("regression") with DualAppIdSet
  with GenerateHtml {
    descr("Performance regression analysis for two sets of Spark application runs (e.g., \"before\" and \"after\")")
    override def genReport(implicit sparkMetricsRetriever: SparkMetricsRetriever): String = {
      if (generateHtml()) {
        SparkAppPerfRegressionReportCli(showStages()).genHtmlReport(AppIdSetPair(appIds1(), appIds2()))
      } else {
        SparkAppPerfRegressionReportCli(showStages()).genTextReport(AppIdSetPair(appIds1(), appIds2()))
      }
    }
  }

  private val helpCmd: Subcommand with HiddenHelp = new Subcommand("help") with HiddenHelp {
    descr("Print usage guidelines")
  }

  addSubcommand(resourceCmd)
  addSubcommand(singleReportCmd)
  addSubcommand(aggReportCmd)
  addSubcommand(diffReportCmd)
  addSubcommand(aggDiffReportCmd)
  addSubcommand(regressionReportCmd)
  addSubcommand(helpCmd)

  override def onError(e: Throwable): Unit = e match {
    case r: ScallopResult if !throwError.value =>
      r match {
        case ScallopException(message) =>
          println("Error parsing arguments.")
          println(message.red.bold)
          printHelp()
          throw e
        case _: Help =>
          printHelp()
      }
    case _ => throw e
  }

  private val usageStr = "GridBench CLI usages: gridbench [-s <shs_endpoint>] <subcommand>".bold.yellow
  banner(s"""$usageStr
            |
            |Example: gridbench -s war report -a application_12345
            |
            |Options:""".stripMargin)

  verify()
}
