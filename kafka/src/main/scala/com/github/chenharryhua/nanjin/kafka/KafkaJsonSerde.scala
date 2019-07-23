package com.github.chenharryhua.nanjin.kafka

import java.util

import cats.Show
import cats.implicits._
import io.circe.syntax._
import io.circe.{parser, Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

import scala.util.{Success, Try}

final case class KafkaJson[A](value: A) extends AnyVal

object KafkaJson {
  import io.circe.generic.semiauto._
  implicit def kafkaJsonDecoder[A: Decoder]: Decoder[KafkaJson[A]] =
    deriveDecoder[KafkaJson[A]]
  implicit def kafkaJsonEncoder[A: Encoder]: Encoder[KafkaJson[A]] =
    deriveEncoder[KafkaJson[A]]
  implicit def showKafkaJson[A: Show]: Show[KafkaJson[A]] =
    _.value.show
}

final class JsonSerde[A: Decoder: Encoder] extends Serde[KafkaJson[A]] {
  private val ser: Serializer[String]     = Serdes.String.serializer()
  private val deSer: Deserializer[String] = Serdes.String.deserializer()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    ser.configure(configs, isKey)
    deSer.configure(configs, isKey)
  }
  override def close(): Unit = {
    ser.close()
    deSer.close()
  }

  override def serializer(): Serializer[KafkaJson[A]] = new Serializer[KafkaJson[A]] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      ser.configure(configs, isKey)
    override def close(): Unit =
      ser.close()

    @SuppressWarnings(Array("AsInstanceOf"))
    override def serialize(topic: String, data: KafkaJson[A]): Array[Byte] =
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) => ser.serialize(topic, d.asJson.noSpaces)
        case None    => null.asInstanceOf[Array[Byte]]
      }
  }
  override def deserializer(): Deserializer[KafkaJson[A]] = new Deserializer[KafkaJson[A]] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      deSer.configure(configs, isKey)
    override def close(): Unit =
      deSer.close()

    @SuppressWarnings(Array("AsInstanceOf"))
    override def deserialize(topic: String, data: Array[Byte]): KafkaJson[A] = {
      val tryDecode: Try[KafkaJson[A]] = Option(data) match {
        case None => Success(KafkaJson(null.asInstanceOf[A]))
        case Some(d) =>
          Try(deSer.deserialize(topic, d))
            .flatMap(
              ds =>
                parser
                  .decode(ds)
                  .leftMap(ex =>
                    DecodingJsonException(
                      s"decode json failed: ${ex.getMessage} source: $ds topic: $topic"))
                  .toTry)
            .map(KafkaJson(_))
      }
      tryDecode.fold(throw _, identity)
    }
  }
}
