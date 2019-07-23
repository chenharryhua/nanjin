package com.github.chenharryhua.nanjin.kafka

import cats.Show
import io.circe.syntax._
import io.circe.{parser, Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

final case class KafkaJson[A](value: A) extends AnyVal

object KafkaJson {
  import io.circe.generic.semiauto._
  implicit def kafkaJsonDecoder[A: Decoder]: Decoder[KafkaJson[A]] = deriveDecoder[KafkaJson[A]]
  implicit def kafkaJsonEncoder[A: Encoder]: Encoder[KafkaJson[A]] = deriveEncoder[KafkaJson[A]]
  implicit def showKafkaJson[A: Encoder]: Show[KafkaJson[A]]       = _.value.asJson.noSpaces
}
@SuppressWarnings(Array("AsInstanceOf"))
final class KafkaJsonSerde[A >: Null: Decoder: Encoder] extends Serde[KafkaJson[A]] {

  override val serializer: Serializer[KafkaJson[A]] =
    (topic: String, data: KafkaJson[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) => d.asJson.noSpaces.getBytes
        case None    => null.asInstanceOf[Array[Byte]]
      }

  @throws
  override val deserializer: Deserializer[KafkaJson[A]] =
    (topic: String, data: Array[Byte]) => {
      val tryDecode: Try[KafkaJson[A]] = Option(data) match {
        case Some(d) => parser.decode[A](new String(d)).map(KafkaJson(_)).toTry
        case None    => Success(null.asInstanceOf[KafkaJson[A]])
      }
      tryDecode.fold(throw _, identity)
    }
}
