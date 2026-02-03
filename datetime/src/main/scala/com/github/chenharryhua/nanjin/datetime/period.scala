package com.github.chenharryhua.nanjin.datetime
import cats.data.{NonEmptyList, Validated}
import cats.parse.{Numbers, Parser, Parser0, Rfc5234}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Period
import scala.util.Try

object period {

  private val y = Parser.ignoreCaseChar('y')
  private val e = Parser.ignoreCaseChar('e')
  private val a = Parser.ignoreCaseChar('a')
  private val r = Parser.ignoreCaseChar('r')
  private val m = Parser.ignoreCaseChar('m')
  private val o = Parser.ignoreCaseChar('o')
  private val n = Parser.ignoreCaseChar('n')
  private val t = Parser.ignoreCaseChar('t')
  private val h = Parser.ignoreCaseChar('h')
  private val d = Parser.ignoreCaseChar('d')
  private val s = Parser.ignoreCaseChar('s').rep0(0, 1)

  private val year: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ y ~ e ~ a ~ r ~ s).void).map(_.toInt)

  private val month: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ m ~ o ~ n ~ t ~ h ~ s).void).map(_.toInt)

  private val day: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ d ~ a ~ y ~ s).void).map(_.toInt)

  private val ymd: Parser0[Period] =
    (year ~ month ~ day <* Parser.end).map { case ((y, m), d) => Period.of(y, m, d) }.backtrack |
      (year ~ month <* Parser.end).map { case (y, m) => Period.of(y, m, 0) }.backtrack |
      (year ~ day <* Parser.end).map { case (y, d) => Period.of(y, 0, d) }.backtrack |
      (month ~ day <* Parser.end).map { case (m, d) => Period.of(0, m, d) }.backtrack |
      (year <* Parser.end).backtrack.map(y => Period.of(y, 0, 0)) |
      (month <* Parser.end).backtrack.map(m => Period.of(0, m, 0)) |
      (day <* Parser.end).map(d => Period.of(0, 0, d))

  private def homebrew(str: String): Validated[NonEmptyList[String], Period] =
    ymd.parse(str) match {
      case Left(value)  => Validated.Invalid(value._2.map(_.offset.toString))
      case Right(value) => Validated.Valid(value._2)
    }

  private def standard(str: String): Validated[NonEmptyList[String], Period] =
    Validated.fromTry(Try(Period.parse(str))).leftMap(ex => NonEmptyList.one(ExceptionUtils.getMessage(ex)))

  def apply(str: String): Validated[NonEmptyList[String], Period] = {
    val trim: String = str.trim
    standard(trim).orElse(homebrew(trim))
  }
}
