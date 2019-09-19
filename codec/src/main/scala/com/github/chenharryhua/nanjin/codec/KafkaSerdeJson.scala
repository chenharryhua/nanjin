package com.github.chenharryhua.nanjin.codec

import cats.implicits._
import cats.{Eq, Show}
import io.circe.syntax._
import io.circe.{parser, Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

final case class KJson[A](value: A) extends AnyVal

object KJson {
  implicit def showKafkaJson[A: Encoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""
  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = cats.derived.semi.eq[KJson[A]]
}

@SuppressWarnings(Array("AsInstanceOf"))
final class KafkaSerdeJson[A: Decoder: Encoder] extends Serde[KJson[A]] {

  override val serializer: Serializer[KJson[A]] =
    (_: String, data: KJson[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) => d.asJson.noSpaces.getBytes
        case None    => null.asInstanceOf[Array[Byte]]
      }

  @throws[CodecException]
  override val deserializer: Deserializer[KJson[A]] =
    (_: String, data: Array[Byte]) => {
      val tryDecode: Try[KJson[A]] = Option(data) match {
        case Some(d) =>
          parser
            .decode[A](new String(d))
            .bimap(e => CodecException.DecodingJsonException(e.show), KJson(_))
            .toTry
        case None => Success(null.asInstanceOf[KJson[A]])
      }
      tryDecode.fold(throw _, identity)
    }
}
