package com.github.chenharryhua.nanjin.kafka.codec

import cats.{Eq, Show}
import io.circe.Encoder
import io.circe.syntax._

final case class KJson[A](value: A) extends AnyVal

object KJson {

  implicit def showKafkaJson[A: Encoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""
  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = cats.derived.semi.eq[KJson[A]]
}
