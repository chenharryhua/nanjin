package com.github.chenharryhua.nanjin.common

import cats.syntax.all.*
import cron4s.Cron
import cron4s.expr.CronExpr
import io.circe.{Decoder, Encoder}
import org.apache.commons.lang3.exception.ExceptionUtils

package object chrono {
  implicit val cronExprEncoder: Encoder[CronExpr] =
    Encoder[String].contramap(_.toString)

  implicit val cronExprDecoder: Decoder[CronExpr] =
    Decoder[String].emap(Cron.parse(_).leftMap(ex => ExceptionUtils.getMessage(ex)))
}
