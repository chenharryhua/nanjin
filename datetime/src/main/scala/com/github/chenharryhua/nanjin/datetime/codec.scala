package com.github.chenharryhua.nanjin.datetime
import java.time.{LocalDate, LocalDateTime}
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

  private def year(ld: LocalDate): String = f"Year=${ld.getYear}"
  private def month(ld: LocalDate): String = f"Month=${ld.getMonthValue}%02d"
  private def day(ld: LocalDate): String = f"Day=${ld.getDayOfMonth}%02d"
  private def hour(ldt: LocalDateTime): String = f"Hour=${ldt.getHour}%02d"
  private def minute(ldt: LocalDateTime): String = f"Minute=${ldt.getMinute}%02d"

  private def parseIntField(str: String, pattern: Regex, min: Int, max: Int): Option[Int] =
    str match {
      case pattern(v) =>
        val value = v.toInt
        Option.when(value >= min && value <= max)(value)
      case _ => None
    }

  def year(str: String): Option[Int] =
    parseIntField(str, """^(?i)Year=(\d{4})""".r, 0, 9999)

  def month(str: String): Option[Int] =
    parseIntField(str, """^(?i)Month=(\d{2})""".r, 1, 12)

  def day(str: String): Option[Int] =
    parseIntField(str, """^(?i)Day=(\d{2})""".r, 1, 31)

  def hour(str: String): Option[Int] =
    parseIntField(str, """^(?i)Hour=(\d{2})""".r, 0, 23)

  def minute(str: String): Option[Int] =
    parseIntField(str, """^(?i)Minute=(\d{2})""".r, 0, 59)

}
