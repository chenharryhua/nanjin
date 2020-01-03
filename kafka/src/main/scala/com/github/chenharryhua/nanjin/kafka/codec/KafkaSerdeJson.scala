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
    (_: String, data: KJson[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) => d.asJson.noSpaces.getBytes
        case None    => null.asInstanceOf[Array[Byte]]
      }

  @throws[CodecException]
  val deserializer: Deserializer[KJson[A]] =
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
