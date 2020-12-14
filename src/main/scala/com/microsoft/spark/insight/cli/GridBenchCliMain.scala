package com.microsoft.spark.insight.cli

import java.net.URI
import java.util.concurrent.TimeUnit

import com.microsoft.spark.insight.cli.argument.{ArgumentConf, ReportGeneration}
import com.microsoft.spark.insight.cli.frontend._
import com.microsoft.spark.insight.utils.spark.CommonStringUtils.{FEEDBACK_LINK, FEEDBACK_LINK_TEMPLATE}
import com.microsoft.spark.insight.utils.spark.SparkHistoryServerMetricsRetriever
import org.rogach.scallop.exceptions.ScallopException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration

/**
 * The entry point to GridBench CLI. Usage can be found in the DocIN documentation
 */
object GridBenchCliMain extends App {
  val LOGGER: Logger = LoggerFactory.getLogger(GridBenchCliMain.getClass)

  private val cliVersion = GridBenchCliMain.getClass.getPackage.getImplementationVersion

  private val versionInfo = cliVersion match {
    case version: String => s"GridBench CLI Version: $version"
    case _ => "No Version"
  }

  private val userHomePath = System.getProperty("user.home")
  println(s"Logging detailed information to $userHomePath/.gridbench/gridbench-cli.log\n$versionInfo\n")
  LOGGER.info(s"$versionInfo")

  private val startTime = System.currentTimeMillis

  private var argumentConf: Option[ArgumentConf] = None

  private val result: Either[Int, Exception] =
    try {
      val argConf = ArgumentConf(args)
      argumentConf = Option(argConf)

      // If the user specifies version option, Cli program returns immediately
      if (argConf.version()) {
        Left(0)
      } else {
        // parse SHS url addr from user input
        val shsURL = GridBenchCliUtil.constructShsUrl(argConf.shsEndpoint())
        implicit val sparkMetricsRetriever: SparkHistoryServerMetricsRetriever =
          new SparkHistoryServerMetricsRetriever(new URI(shsURL))

        // Match sub command to trigger different service
        argConf.subcommand match {
          case Some(subcommand: ReportGeneration) =>
            val output = subcommand.genReport
            println(output)
            Left(output.length)
          case _ =>
            argConf.printHelp()
            Left(0)
        }
      }
    } catch {
      case e: Exception =>
        val message = s"GridBench CLI failed. Reason: $e"
        if (!e.isInstanceOf[ScallopException]) {
          // Silence these known exceptions, as we don't want users to receive meaningless stack traces, but still log
          println(s"[Error] \u274C $message")
        }
        LOGGER.error(message)
        Right(e)
    }

  private val duration = Duration(System.currentTimeMillis - startTime, TimeUnit.MILLISECONDS)
  println(s"\nTotal processing time: ${duration.toSeconds} seconds")
  println(s"$FEEDBACK_LINK_TEMPLATE ${FEEDBACK_LINK.blue.bold}")

  // Empty out the buffer to the cli screen
  Console.out.flush()

}
