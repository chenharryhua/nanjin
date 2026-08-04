package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.syntax.show.showInterpolator

import scala.io.AnsiColor

sealed private trait LogColor:
  def good: Endo[String]
  def info: Endo[String]
  def warn: Endo[String]
  def error: Endo[String]
  def debug: Endo[String]
end LogColor

private object LogColor:
  private def colorize(code: String)(name: String): String =
    show"$code$name${AnsiColor.RESET}"

  val render: LogColor = new LogColor {
    override val good: Endo[String] = colorize(AnsiColor.GREEN)
    override val info: Endo[String] = colorize(AnsiColor.CYAN)
    override val warn: Endo[String] = colorize(AnsiColor.YELLOW)
    override val error: Endo[String] = colorize(AnsiColor.RED)
    override val debug: Endo[String] = colorize(AnsiColor.MAGENTA)
  }

  val none: LogColor = new LogColor {
    override val good: Endo[String] = identity
    override val info: Endo[String] = identity
    override val warn: Endo[String] = identity
    override val error: Endo[String] = identity
    override val debug: Endo[String] = identity
  }
end LogColor
