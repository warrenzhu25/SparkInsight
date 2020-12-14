package com.microsoft.spark.insight.cli.frontend

/**
 * Define common position of a table divider format
 * @param format table divider format
 */
private[frontend] final case class TableDividerFormat(format: String) {
  require(format != null && format.length == 5, "string format must be at length of 5")

  def leftBorder: Char = format.charAt(0)

  def middleBoundary: Char = format.charAt(2)

  def horizontalDiv: Char = format.charAt(1)

  def rightBorder: Char = format.charAt(4)
}

/**
 * Common table dividers including header and data formats
 */
private[frontend] object TableDividerFormat {

  val headerTopFormat: TableDividerFormat = TableDividerFormat("╔═╤═╗")

  val headerBottomFormat: TableDividerFormat = TableDividerFormat("╠═╪═╣")

  val dataRowFormat: TableDividerFormat = TableDividerFormat("╟─┼─╢")

  val dataRowBottomFormat: TableDividerFormat = TableDividerFormat("╚═╧═╝")

  val headerDivider = '║'

  val dataDivider = '│'
}
