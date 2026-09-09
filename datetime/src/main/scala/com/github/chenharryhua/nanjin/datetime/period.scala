package com.github.chenharryhua.nanjin.datetime
import cats.data.{NonEmptyList, Validated}
import cats.parse.{Numbers, Parser, Parser0, Rfc5234}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Period
import scala.util.Try

/** Parses a `String` into a `java.time.Period`.
  *
  * Two syntaxes are accepted, tried in order by `apply`: the standard ISO-8601 form understood by
  * `Period.parse` (e.g. `"P1Y2M3D"`), and a lenient homebrew form spelling out the units (e.g.
  * `"1 year 2 months 3 days"`) built with `cats-parse`. Results are returned as a `Validated` whose invalid
  * side carries failure messages rather than throwing.
  */
object period {

  /* Case-insensitive single-character parsers used to spell out the unit keywords "years",
   * "months", and "days". `s` matches an optional trailing "s" so both singular and plural forms
   * are accepted. */

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

  /** Parses a count of years: optional leading spaces, digits, then the space-tolerant word `year`/`years`.
    */
  private val year: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ y ~ e ~ a ~ r ~ s).void).map(_.toInt)

  /** Parses a count of months: optional leading spaces, digits, then `month`/`months`. */
  private val month: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ m ~ o ~ n ~ t ~ h ~ s).void).map(_.toInt)

  /** Parses a count of days: optional leading spaces, digits, then `day`/`days`. */
  private val day: Parser0[Int] =
    (Rfc5234.sp.rep0 *> Numbers.digits <* (Rfc5234.sp.rep0 ~ d ~ a ~ y ~ s).void).map(_.toInt)

  /** The homebrew grammar: accepts year/month/day in that order, any two of them, or any single one. Each
    * alternative is anchored at end-of-input and backtracks so the next combination can be attempted; omitted
    * fields default to zero.
    */
  private val ymd: Parser0[Period] =
    (year ~ month ~ day <* Parser.end).map { case ((y, m), d) => Period.of(y, m, d) }.backtrack |
      (year ~ month <* Parser.end).map { case (y, m) => Period.of(y, m, 0) }.backtrack |
      (year ~ day <* Parser.end).map { case (y, d) => Period.of(y, 0, d) }.backtrack |
      (month ~ day <* Parser.end).map { case (m, d) => Period.of(0, m, d) }.backtrack |
      (year <* Parser.end).backtrack.map(y => Period.of(y, 0, 0)) |
      (month <* Parser.end).backtrack.map(m => Period.of(0, m, 0)) |
      (day <* Parser.end).map(d => Period.of(0, 0, d))

  /** Runs the homebrew `ymd` grammar, reporting the failing input offset(s) as messages on the invalid side.
    */
  private def homebrew(str: String): Validated[NonEmptyList[String], Period] =
    ymd.parse(str) match {
      case Left(value)  => Validated.Invalid(value._2.map(_.offset.toString))
      case Right(value) => Validated.Valid(value._2)
    }

  /** Parses the ISO-8601 form via `Period.parse`, capturing the exception message on failure. */
  private def standard(str: String): Validated[NonEmptyList[String], Period] =
    Validated.fromTry(Try(Period.parse(str))).leftMap(ex => NonEmptyList.one(ExceptionUtils.getMessage(ex)))

  /** Parses `str` (after trimming) as a `Period`, trying the standard ISO-8601 form first and falling back to
    * the homebrew form.
    */
  def apply(str: String): Validated[NonEmptyList[String], Period] = {
    val trim: String = str.trim
    standard(trim).orElse(homebrew(trim))
  }
}
