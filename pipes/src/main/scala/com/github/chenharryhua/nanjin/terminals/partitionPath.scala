package com.github.chenharryhua.nanjin.terminals

import java.time.{LocalDate, LocalDateTime}
import scala.util.matching.Regex

object partitionPath:

  private case class Field(name: String, width: Int, min: Int, max: Int):
    private val pattern: Regex = s"(?i)^$name=(\\d{$width})$$".r

    def format(value: Int): String =
      s"$name=${s"%0${width}d".formatted(value)}"

    def parse(str: String): Option[Int] =
      str match
        case pattern(v) =>
          val i = v.toInt
          Option.when(i >= min && i <= max)(i)
        case _ => None

  // -------- field definitions --------
  private val Year = Field("Year", 4, 0, 9999)
  private val Month = Field("Month", 2, 1, 12)
  private val Day = Field("Day", 2, 1, 31)
  private val Hour = Field("Hour", 2, 0, 23)
  private val Minute = Field("Minute", 2, 0, 59)

  // -------- formatting --------

  def ymd(ld: LocalDate): String =
    List(
      Year.format(ld.getYear),
      Month.format(ld.getMonthValue),
      Day.format(ld.getDayOfMonth)
    ).mkString("/")

  def ymdh(ldt: LocalDateTime): String =
    ymd(ldt.toLocalDate) + "/" + Hour.format(ldt.getHour)

  def ymdhm(ldt: LocalDateTime): String =
    ymdh(ldt) + "/" + Minute.format(ldt.getMinute)

  // -------- parsing --------

  def year(str: String): Option[Int] = Year.parse(str)
  def month(str: String): Option[Int] = Month.parse(str)
  def day(str: String): Option[Int] = Day.parse(str)
  def hour(str: String): Option[Int] = Hour.parse(str)
  def minute(str: String): Option[Int] = Minute.parse(str)

end partitionPath
