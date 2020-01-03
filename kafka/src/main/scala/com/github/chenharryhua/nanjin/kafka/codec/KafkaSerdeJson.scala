package com.github.chenharryhua.nanjin.kafka.codec

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KJson
import io.circe.syntax._
import io.circe.{parser, Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

@SuppressWarnings(Array("AsInstanceOf"))
final private[codec] class KafkaSerdeJson[A: Decoder: Encoder]
    extends Serde[KJson[A]] with Serializable {

  val serializer: Serializer[KJson[A]] =
    new Serializer[KJson[A]] with Serializable {

      override def serialize(topic: String, data: KJson[A]): Array[Byte] =
        Option(data).flatMap(x => Option(x.value)) match {
          case Some(d) => d.asJson.noSpaces.getBytes
          case None    => null.asInstanceOf[Array[Byte]]
        }
    }

  @throws[CodecException]
  val deserializer: Deserializer[KJson[A]] =
    new Deserializer[KJson[A]] with Serializable {

      override def deserialize(topic: String, data: Array[Byte]): KJson[A] = {
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
}
