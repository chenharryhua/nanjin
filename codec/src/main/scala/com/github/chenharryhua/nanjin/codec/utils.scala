package com.github.chenharryhua.nanjin.codec

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Properties

import cats.Eval

import scala.util.Random
import java.sql.Timestamp

// in unit of milli-second
final case class KafkaTimestamp(ts: Long, tz: ZoneId) {
  def utc: Instant             = Instant.ofEpochMilli(ts)
  def local: ZonedDateTime     = utc.atZone(tz)
  def javaLong: java.lang.Long = ts
}

object KafkaTimestamp {
  private val zoneId: ZoneId = ZoneId.systemDefault()

  def apply(ts: Long): KafkaTimestamp          = KafkaTimestamp(ts, zoneId)
  def apply(ts: Timestamp): KafkaTimestamp     = KafkaTimestamp(ts.getTime, zoneId)
  def apply(utc: Instant): KafkaTimestamp      = KafkaTimestamp(utc.toEpochMilli)
  def apply(ts: LocalDateTime): KafkaTimestamp = apply(ts.atZone(zoneId).toInstant)
  def apply(ts: ZonedDateTime): KafkaTimestamp = apply(ts.toInstant)
}

object utils {

  //kafka was graduated from apache incubator
  val kafkaEpoch: LocalDateTime = LocalDateTime.of(2012, 10, 23, 0, 0, 0)

  def toProperties(props: Map[String, String]): Properties =
    props.foldLeft(new Properties()) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))
}
