package com.microsoft.spark.insight.cli

/**
 * Utility object for gridbench-cli module
 */
object GridBenchCliUtil {

  /**
   * Convert number of GB/MB/KB in String to number of gigabytes in Double value.
   *
   * @return value in Gigabytes
   */
  def strToGB(x: String): Double = {
    val pattern = "([0-9]+)([kgm]b?)".r
    val patternMB = "([0-9]+)".r

    x.split("\\s+").mkString.toLowerCase() match {
      case pattern(num, unit) =>
        unit.charAt(0) match {
          case 'k' => num.toInt / 1024d / 1024d
          case 'm' => num.toInt / 1024d
          case 'g' => num.toDouble
        }
      case patternMB(num) => num.toInt / 1024d

      // If Scala throws Match Error, throw an exception
      case _ => throw new IllegalArgumentException("The input string cannot be parsed to GB value.")
    }
  }

  /**
   * Calculate memory overhead which starts from 0.375GB, and grows as the executor size grows (10%)*
   * more detailed: go/sparkmemory
   *
   * @return executor memory overhead value in Gigabytes
   */
  def getMemoryOverheadInGB(exeMemory: Double, overheadMemory: String): Double = {
    overheadMemory match {
      case " " | null => Math.max(exeMemory * .1, 0.375d)
      case _ => strToGB(overheadMemory)
    }
  }

  private val clusterShsUrlMap = Map(
    "faro" -> "http://ltx1-farosh01.grid.linkedin.com:18080",
    "holdem" -> "http://ltx1-holdemsh01.grid.linkedin.com:18080",
    "lasso" -> "http://ltx1-lassojh01.grid.linkedin.com:18080",
    "mlearn-alpha" -> "http://ltx1-mlearn-alpha-jh01.grid.linkedin.com:18080",
    "pokemon" -> "http://lva1-pokemonsh01.grid.linkedin.com:18080",
    "war" -> "http://lva1-warsh01.grid.linkedin.com:18080",
    "yugioh" -> "http://ltx1-yugiohsh01.grid.linkedin.com:18080")

  /**
   * Construct a URL from the provided user input. If the input is a known Grid cluster name, this method will expand it
   * to the full SHS URL, otherwise will return the URL that was provided as-is
   *
   * @param str user input
   * @return Full SHS URL
   */
  def constructShsUrl(str: String): String = {
    clusterShsUrlMap.get(str.toLowerCase) match {
      case Some(url) => url
      case None => str
    }
  }
}
