package com.github.chenharryhua.nanjin.codec

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Properties

import cats.Eval

import scala.util.Random

final private[codec] case class NJTimestamp(ts: Long, tz: ZoneId) {
  val utc: Instant         = Instant.ofEpochMilli(ts)
  val local: ZonedDateTime = utc.atZone(tz)
}

private[codec] object NJTimestamp {
  def apply(ts: Long): NJTimestamp = new NJTimestamp(ts, ZoneId.systemDefault())
}

object utils {

  def toProperties(props: Map[String, String]): Properties =
    (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))

  def localDateTime2KafkaTimestamp(dt: LocalDateTime, tz: ZoneId = ZoneId.systemDefault()): Long =
    dt.atZone(tz).toInstant.toEpochMilli

}
