package com.github.chenharryhua.nanjin.guard.logging

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel

import scala.io.AnsiColor

sealed private trait LogColor:
  def good: String => String
  def info: String => String
  def warn: String => String
  def error: String => String
  def debug: String => String
end LogColor


private object LogColor {
  private def colorize(alarm: AlarmLevel, code: String)(name: String): String =
    show"${alarm.level.name()} -- $code$name${AnsiColor.RESET}"

  val console: LogColor = new LogColor {
    override val good: String => String = colorize(AlarmLevel.Good, AnsiColor.GREEN)
    override val info: String => String = colorize(AlarmLevel.Info, AnsiColor.CYAN)
    override val warn: String => String = colorize(AlarmLevel.Warn, AnsiColor.YELLOW)
    override val error: String => String = colorize(AlarmLevel.Error, AnsiColor.RED)
    override val debug: String => String = colorize(AlarmLevel.Debug, AnsiColor.MAGENTA)
  }

  val none: LogColor = new LogColor {
    override val good: String => String = identity
    override val info: String => String = identity
    override val warn: String => String = identity
    override val error: String => String = identity
    override val debug: String => String = identity
  }
}
