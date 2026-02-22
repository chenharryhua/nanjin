package com.github.chenharryhua.nanjin.guard.logging

import scala.io.AnsiColor

sealed private trait LogColor {
  def done: String => String
  def info: String => String
  def warn: String => String
  def error: String => String
  def debug: String => String
}

private object LogColor {
  private def colorize(code: String)(name: String): String = code + name + AnsiColor.RESET

  val standard: LogColor = new LogColor {
    override val done: String => String = colorize(AnsiColor.GREEN)
    override val info: String => String = colorize(AnsiColor.CYAN)
    override val warn: String => String = colorize(AnsiColor.YELLOW)
    override val error: String => String = colorize(AnsiColor.RED)
    override val debug: String => String = colorize(AnsiColor.MAGENTA)
  }

  val none: LogColor = new LogColor {
    override val done: String => String = identity
    override val info: String => String = identity
    override val warn: String => String = identity
    override val error: String => String = identity
    override val debug: String => String = identity
  }
}
