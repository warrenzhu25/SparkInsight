package com.microsoft.spark.insight.cli.frontend

/**
 * Used for building a table from Array[String] with text style modifications
 */
private final class CliTable(
    headers: Array[String],
    dataRows: Array[Array[String]],
    headerStyling: Map[Int, Seq[TextStyling]],
    dataStyling: Map[(Int, Int), Seq[TextStyling]]) {

  private val nCol: Int = headers.length

  // Maintain the width of each column in the table
  private val colWidths: Array[Int] = new Array[Int](nCol)

  // Trying to evaluate the width of every column, by finding the maximum width of all cells in a column
  for {
    row <- headers +: dataRows
    col <- 0 until nCol
    cell <- row(col).split("\\n")
  } {
    colWidths(col) = Math.max(colWidths(col), cell.length)
  }

  /**
   * Print the Table by overriding toString method
   * @return formatted string table
   */
  override def toString: String = {
    val builder = new TableBuilder(new StringBuilder)
    builder.append("\n")
    builder.printDivider(TableDividerFormat.headerTopFormat)
    builder.printContent(headers, 0, isHeader = true)
    for (rowIndex <- dataRows.indices) {

      if (rowIndex == 0) {
        builder.printDivider(TableDividerFormat.headerBottomFormat)
      } else {
        builder.printDivider(TableDividerFormat.dataRowFormat)
      }

      builder.printContent(dataRows(rowIndex), rowIndex, isHeader = false)
    }
    builder.printDivider(TableDividerFormat.dataRowBottomFormat)
    builder.toString
  }

  /**
   * Table string builder. Also Used for clear abstraction of CliTable construction.
   */
  final class TableBuilder(builder: StringBuilder) {

    require(builder != null, "string builder cannot be null.")

    def append(s: Any): StringBuilder = builder.append(s)

    // Print divider of the Table
    def printDivider(format: TableDividerFormat): Unit = {
      for (col <- 0 until nCol) {
        if (col == 0) {
          append(format.leftBorder)
        } else {
          append(format.middleBoundary)
        }
        append(pad(colWidths(col), "").replace(' ', format.horizontalDiv))
      }
      append(format.rightBorder).append('\n')
    }

    // Print one cell of the Table, either one header or one cell of data
    def printContent(cell: Seq[String], rowIndex: Int, isHeader: Boolean): Unit = {

      // Find out the maximum of lines for any column data in this row
      val maxCellLine: Int = cell.map(_.split("\\n").length).max

      for (line <- 0 until maxCellLine) {
        for (colIndex <- 0 until nCol) {
          append(if (colIndex == 0) TableDividerFormat.headerDivider else TableDividerFormat.dataDivider)
          val cellLines: Array[String] = cell(colIndex).split("\\n")

          val cellLine: String =
            if (line < cellLines.length) cellLines(line) else ""

          val next = pad(colWidths(colIndex), cellLine)

          val textStylingChanges: Seq[TextStyling] = if (isHeader) {
            headerStyling.getOrElse(colIndex, CliTable.headerDefaultStyling)
          } else {
            dataStyling.getOrElse((rowIndex, colIndex), CliTable.dataDefaultStyling)
          }

          val colorString = TextStyling.handleAll((next, textStylingChanges))
          append(colorString)
        }
        append(TableDividerFormat.headerDivider)
        append("\n")
      }
    }

    // Format string with the specified width
    private def pad(width: Int, data: String): String = {
      String.format(" %1$-" + width + "s ", data)
    }

    override def toString: String = builder.toString
  }
}

/**
 * We provide two API for table creations
 */
object CliTable {

  val headerDefaultStyling: Seq[TextStyling] = Seq(TextStyling("YELLOW"), TextStyling("BOLD"))
  val dataDefaultStyling: Seq[TextStyling] = Seq(TextStyling("BLACK"))

  /**
   * to create a Cli Table with customized text style
   *
   * @param headers table headers with the layout of Array[String]
   * @param data table contents
   * @param headerStyling text styling spec for headers
   * @param dataStyling text styling spec for data with (row, col) coordinate
   * @return formatted table with text styling settings
   */
  def createWithTextStyling(
      headers: Array[String],
      data: Array[Array[String]],
      headerStyling: Map[Int, Seq[TextStyling]],
      dataStyling: Map[(Int, Int), Seq[TextStyling]]): String = {

    require(headers != null, "Table headers cannot be null.")
    require(headers.length > 0, "There must be at least 1 column.")
    require(data != null, "Table contents cannot be null")

    // Guarantee that header and all data has the same length of data
    require(
      data.forall(_.length == headers.length),
      "Header column number should align with every row in Table content.")

    new CliTable(headers, data, headerStyling, dataStyling).toString()
  }

  /**
   * to create a Cli Table with default color format
   *
   * @param headers table headers with the layout of Array[String]
   * @param data table contents
   * @return formatted table with default text styling settings
   */
  def create(headers: Array[String], data: Array[Array[String]]): String = {
    createWithTextStyling(headers, data, Map.empty, Map.empty)
  }
}
