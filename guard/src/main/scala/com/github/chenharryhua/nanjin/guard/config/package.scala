package com.github.chenharryhua.nanjin.guard

import cats.syntax.either.*
import cron4s.expr.CronExpr
import cron4s.Cron
import io.circe.{Decoder, Encoder}
import org.apache.commons.lang3.exception.ExceptionUtils

package object config {
  implicit val cronExprEncoder: Encoder[CronExpr] = Encoder[String].contramap(_.toString)
  implicit val cronExprDecoder: Decoder[CronExpr] =
    Decoder[String].emap(Cron.parse(_).leftMap(ex => ExceptionUtils.getMessage(ex)))

  val dailyCron: CronExpr   = Cron.unsafeParse("1 0 0 ? * *")
  val weeklyCron: CronExpr  = Cron.unsafeParse("1 0 0 ? * 0")
  val monthlyCron: CronExpr = Cron.unsafeParse("1 0 0 1 * ?")

}
