package com.github.chenharryhua.nanjin.datetime
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try
import scala.util.matching.Regex

object codec {
  def year_month_day(ld: LocalDate): String =
    s"${year(ld)}/${month(ld)}/${day(ld)}"

  def year_month_day_hour(ldt: LocalDateTime): String = {
    val ld = ldt.toLocalDate
    s"${year(ld)}/${month(ld)}/${day(ld)}/${hour(ldt)}"
  }

  def year_month_day_hour_minute(ldt: LocalDateTime): String = {
    val ld = ldt.toLocalDate
    s"${year(ld)}/${month(ld)}/${day(ld)}/${hour(ldt)}/${minute(ldt)}"
  }

  private def year(ld: LocalDate): String        = f"Year=${ld.getYear}"
  private def month(ld: LocalDate): String       = f"Month=${ld.getMonthValue}%02d"
  private def day(ld: LocalDate): String         = f"Day=${ld.getDayOfMonth}%02d"
  private def hour(ldt: LocalDateTime): String   = f"Hour=${ldt.getHour}%02d"
  private def minute(ldt: LocalDateTime): String = f"Minute=${ldt.getMinute}%02d"

  def year(str: String): Option[Int] = {
    val patten: Regex = """^Year=(\d{4})""".r
    str match {
      case patten(y) => Try(y.toInt).toOption
      case _         => None
    }
  }

  def month(str: String): Option[Int] = {
    val patten: Regex = """^Month=(\d{2})""".r
    str match {
      case patten(m) => Try(m.toInt).toOption
      case _         => None
    }
  }

  def day(str: String): Option[Int] = {
    val patten: Regex = """^Day=(\d{2})""".r
    str match {
      case patten(d) => Try(d.toInt).toOption
      case _         => None
    }
  }

  def hour(str: String): Option[Int] = {
    val patten: Regex = """^Hour=(\d{2})""".r
    str match {
      case patten(h) => Try(h.toInt).toOption
      case _         => None
    }
  }

  def minute(str: String): Option[Int] = {
    val patten: Regex = """^Minute=(\d{2})""".r
    str match {
      case patten(m) => Try(m.toInt).toOption
      case _         => None
    }
  }
}
