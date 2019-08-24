package com.github.chenharryhua.nanjin.kafka

import cats.Show
import io.circe.syntax._
import io.circe.{parser, Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

final case class KJson[A](value: A) extends AnyVal

object KJson {
  import io.circe.generic.semiauto._
  implicit def kafkaJsonDecoder[A: Decoder]: Decoder[KJson[A]] = deriveDecoder[KJson[A]]
  implicit def kafkaJsonEncoder[A: Encoder]: Encoder[KJson[A]] = deriveEncoder[KJson[A]]
  implicit def showKafkaJson[A: Encoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"KJson(${t.value.asJson.noSpaces})"
}

@SuppressWarnings(Array("AsInstanceOf"))
final class KafkaJsonSerde[A: Decoder: Encoder] extends Serde[KJson[A]] {

  override val serializer: Serializer[KJson[A]] =
    (_: String, data: KJson[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) => d.asJson.noSpaces.getBytes
        case None    => null.asInstanceOf[Array[Byte]]
      }

  @throws
  override val deserializer: Deserializer[KJson[A]] =
    (_: String, data: Array[Byte]) => {
      val tryDecode: Try[KJson[A]] = Option(data) match {
        case Some(d) => parser.decode[A](new String(d)).map(KJson(_)).toTry
        case None    => Success(null.asInstanceOf[KJson[A]])
      }
      tryDecode.fold(throw _, identity)
    }
}
