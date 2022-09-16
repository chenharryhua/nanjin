package com.github.chenharryhua.nanjin.guard

import cron4s.expr.CronExpr
import cron4s.Cron
import io.circe.{Decoder, Encoder}
import cats.syntax.either.*

package object config {
  implicit val cronExprEncoder: Encoder[CronExpr] = Encoder[String].contramap(_.toString)
  implicit val cronExprDecoder: Decoder[CronExpr] = Decoder[String].emap(Cron.parse(_).leftMap(_.getMessage))
}
