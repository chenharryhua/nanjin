package com.github.chenharryhua.nanjin.codec

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Properties

import cats.Eval

import scala.util.{Failure, Random, Success, Try}

object utils {

  def toProperties(props: Map[String, String]): Properties =
    (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  val random4d: Eval[Int] = Eval.always(1000 + Random.nextInt(9000))

  def kafkaTimestamp(t: Long, tz: ZoneId = ZoneId.systemDefault()): (Instant, ZonedDateTime) = {
    val utc = Instant.ofEpochMilli(t)
    (utc, utc.atZone(tz))
  }

  def kafkaTimestamp2LocalDateTime(ts: Long, tz: ZoneId = ZoneId.systemDefault()): LocalDateTime =
    LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), tz)

  def checkNull[A](a: A): Try[A] =
    Option(a).fold[Try[A]](Failure(CodecException.DecodingNullException))(Success(_))
}
