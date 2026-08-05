package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.syntax.show.showInterpolator

import scala.io.AnsiColor

sealed private trait ColorMode:
  def good: Endo[String]
  def info: Endo[String]
  def warn: Endo[String]
  def error: Endo[String]
  def debug: Endo[String]
end ColorMode

private object ColorMode:
  private def colorize(code: String)(text: String): String =
    show"$code$text${AnsiColor.RESET}"

  val render: ColorMode = new ColorMode {
    override val good: Endo[String] = colorize(AnsiColor.GREEN)
    override val info: Endo[String] = colorize(AnsiColor.CYAN)
    override val warn: Endo[String] = colorize(AnsiColor.YELLOW)
    override val error: Endo[String] = colorize(AnsiColor.RED)
    override val debug: Endo[String] = colorize(AnsiColor.MAGENTA)
  }

  val none: ColorMode = new ColorMode {
    override val good: Endo[String] = identity
    override val info: Endo[String] = identity
    override val warn: Endo[String] = identity
    override val error: Endo[String] = identity
    override val debug: Endo[String] = identity
  }
end ColorMode
