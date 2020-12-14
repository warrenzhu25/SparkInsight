package com.microsoft.spark.insight.cli.frontend

/**
 * A wrapper case class to include one style change
 *
 * @param textStyle one text style change
 */
final case class TextStyling(textStyle: String)

/**
 * TextStyling Utilities
 */
object TextStyling {

  // Transform a string with one text style change. Ignore transformation if it is not defined
  private val handler: PartialFunction[TextStylingString, String] = {
    case (s, BLACK) => s
    case (s, RED) => s.red
    case (s, GREEN) => s.green
    case (s, YELLOW) => s.yellow
    case (s, BLUE) => s.blue
    case (s, BLACK_B) => s.onBlack
    case (s, RED_B) => s.onRed
    case (s, GREEN_B) => s.onGreen
    case (s, YELLOW_B) => s.onYellow
    case (s, BLUE_B) => s.onBlue
    case (s, BOLD) => s.bold
    case (s, UNDERLINED) => s.underlined
    case (s, _) => s
  }

  /**
   * transform TextStyling change to a string
   *
   * @param textStylingString one TextStyling transformation
   */
  def handle(textStylingString: TextStylingString): String = handler(textStylingString)

  /**
   * transform all TextStyling changes to a string
   *
   * @param multiTextStylingString all TextStyling transformations
   */
  def handleAll(multiTextStylingString: MultiTextStylingString): String = {
    multiTextStylingString._2.foldLeft(multiTextStylingString._1) {
      case tuple: TextStylingString => handle(tuple)
    }
  }

  val BLACK: TextStyling = TextStyling("BLACK")
  val RED: TextStyling = TextStyling("RED")
  val GREEN: TextStyling = TextStyling("GREEN")
  val YELLOW: TextStyling = TextStyling("YELLOW")
  val BLUE: TextStyling = TextStyling("BLUE")
  val BLACK_B: TextStyling = TextStyling("BLACK_B")
  val RED_B: TextStyling = TextStyling("RED_B")
  val GREEN_B: TextStyling = TextStyling("GREEN_B")
  val YELLOW_B: TextStyling = TextStyling("YELLOW_B")
  val BLUE_B: TextStyling = TextStyling("BLUE_B")
  val BOLD: TextStyling = TextStyling("BOLD")
  val UNDERLINED: TextStyling = TextStyling("UNDERLINED")
}
