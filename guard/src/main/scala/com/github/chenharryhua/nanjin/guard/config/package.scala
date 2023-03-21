package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.syntax.either.*
import cron4s.Cron
import cron4s.expr.CronExpr
import io.circe.{Decoder, Encoder, Json}
import io.scalaland.enumz.Enum
import org.apache.commons.lang3.exception.ExceptionUtils
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

package object config {
  implicit val cronExprEncoder: Encoder[CronExpr] = Encoder[String].contramap(_.toString)
  implicit val cronExprDecoder: Decoder[CronExpr] =
    Decoder[String].emap(Cron.parse(_).leftMap(ex => ExceptionUtils.getMessage(ex)))

  val dailyCron: CronExpr   = Cron.unsafeParse("1 0 0 ? * *")
  val weeklyCron: CronExpr  = Cron.unsafeParse("1 0 0 ? * 0")
  val monthlyCron: CronExpr = Cron.unsafeParse("1 0 0 1 * ?")

  private val esu: Enum[StandardUnit] = Enum[StandardUnit]
  implicit val standardUnitEncoder: Encoder[StandardUnit] =
    Encoder.instance(su => Json.fromString(esu.getName(su)))

  implicit val standardUnitDecoder: Decoder[StandardUnit] =
    Decoder.decodeString.emap(u =>
      esu.withNameOption(u) match {
        case Some(value) => Right(value)
        case None        => Left(s"$u is an invalid CloudWatch StandardUnit")
      })

  implicit val showStandardUnit: Show[StandardUnit] = esu.getName(_).toLowerCase()
}
