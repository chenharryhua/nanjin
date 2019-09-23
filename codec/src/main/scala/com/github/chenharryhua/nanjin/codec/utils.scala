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

  //kafka was graduated from apache incubator
  val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties =
    props.foldLeft(new Properties()) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))

  def kafkaTimestamp(ts: Long, tz: ZoneId = ZoneId.systemDefault()): ZonedDateTime =
    Instant.ofEpochMilli(ts).atZone(tz)

  def localDateTime2KafkaTimestamp(dt: LocalDateTime, tz: ZoneId = ZoneId.systemDefault()): Long =
    dt.atZone(tz).toInstant.toEpochMilli

}
