package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.logging.LogLevel

import scala.io.AnsiColor

sealed private trait LogColor:
  def good: Endo[String]
  def info: Endo[String]
  def warn: Endo[String]
  def error: Endo[String]
  def debug: Endo[String]
end LogColor

private object LogColor:
  private def colorize(level: LogLevel, code: String)(name: String): String =
    show"${level.name} -- $code$name${AnsiColor.RESET}"

  val console: LogColor = new LogColor {
    override val good: Endo[String] = colorize(LogLevel.Good, AnsiColor.GREEN)
    override val info: Endo[String] = colorize(LogLevel.Info, AnsiColor.CYAN)
    override val warn: Endo[String] = colorize(LogLevel.Warn, AnsiColor.YELLOW)
    override val error: Endo[String] = colorize(LogLevel.Error, AnsiColor.RED)
    override val debug: Endo[String] = colorize(LogLevel.Debug, AnsiColor.MAGENTA)
  }

  val none: LogColor = new LogColor {
    override val good: Endo[String] = identity
    override val info: Endo[String] = identity
    override val warn: Endo[String] = identity
    override val error: Endo[String] = identity
    override val debug: Endo[String] = identity
  }
end LogColor
