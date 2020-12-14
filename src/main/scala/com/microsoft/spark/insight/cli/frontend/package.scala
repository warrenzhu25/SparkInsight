package com.microsoft.spark.insight.cli

/**
 * frontend types and utilities shared in this package
 *
 * To use this color utility in other modules, simply do the following:
 * '''
 * import frontend._
 * println { "Red warning".red }
 * '''
 */
package object frontend {

  type TextStylingString = (String, TextStyling)

  type MultiTextStylingString = (String, Seq[TextStyling])

  /**
   * Make terminal Output coloring for Scala
   *
   * @param s string to apply TextStyling changes
   */
  implicit class TextStylingUtility(s: String) {
    import Console._

    /** Colorize the given string foreground to ANSI black */
    def black = BLACK + s + RESET

    /** Colorize the given string foreground to ANSI red */
    def red = RED + s + RESET

    /** Colorize the given string foreground to ANSI green */
    def green = GREEN + s + RESET

    /** Colorize the given string foreground to ANSI yellow */
    def yellow = YELLOW + s + RESET

    /** Colorize the given string foreground to ANSI blue */
    def blue = BLUE + s + RESET

    /** Colorize the given string background to ANSI black */
    def onBlack = BLACK_B + s + RESET

    /** Colorize the given string background to ANSI red */
    def onRed = RED_B + s + RESET

    /** Colorize the given string background to ANSI green */
    def onGreen = GREEN_B + s + RESET

    /** Colorize the given string background to ANSI yellow */
    def onYellow = YELLOW_B + s + RESET

    /** Colorize the given string background to ANSI blue */
    def onBlue = BLUE_B + s + RESET

    /** Make the given string bold */
    def bold = BOLD + s + RESET

    /** Underline the given string */
    def underlined = UNDERLINED + s + RESET
  }
}
