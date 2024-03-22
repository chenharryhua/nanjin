package com.github.chenharryhua.nanjin.common

import cats.implicits.toBifunctorOps
import cron4s.Cron
import cron4s.expr.CronExpr
import io.circe.{Decoder, Encoder}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.math.pow

package object chrono {
  implicit val cronExprEncoder: Encoder[CronExpr] = Encoder[String].contramap(_.toString)
  implicit val cronExprDecoder: Decoder[CronExpr] =
    Decoder[String].emap(Cron.parse(_).leftMap(ex => ExceptionUtils.getMessage(ex)))

  val fibonacci: LazyList[Long]   = 1L #:: 1L #:: fibonacci.zip(fibonacci.tail).map { case (a, b) => a + b }
  val exponential: LazyList[Long] = LazyList.from(0).map(x => pow(2, x.toDouble).toLong)

  def lazyTickList(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts => ts.next(ts.tick.wakeup).map(s => (s.tick, s)))

}
