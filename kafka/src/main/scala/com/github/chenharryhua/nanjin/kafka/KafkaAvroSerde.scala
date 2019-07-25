package com.github.chenharryhua.nanjin.kafka
import java.io.ByteArrayOutputStream

import cats.Show
import cats.implicits._
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

final case class KAvro[A](value: A) extends AnyVal

object KAvro {
  implicit def showKafkaAvro[A: Show]: Show[KAvro[A]] = _.value.show
}

@SuppressWarnings(Array("AsInstanceOf"))
final class KafkaAvroSerde[A: Decoder: Encoder](schema: Schema)
    extends Serde[KAvro[A]] with Serializable {

  override val serializer: Serializer[KAvro[A]] =
    (_: String, data: KAvro[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) =>
          val baos   = new ByteArrayOutputStream()
          val output = AvroOutputStream.binary[A].to(baos).build(schema)
          output.write(d)
          output.close()
          baos.toByteArray
        case None => null.asInstanceOf[Array[Byte]]
      }

  override val deserializer: Deserializer[KAvro[A]] =
    (_: String, data: Array[Byte]) =>
      Option(data) match {
        case Some(d) =>
          val input = AvroInputStream.binary[A].from(d).build(schema)
          KAvro(input.iterator.next)
        case None => null.asInstanceOf[KAvro[A]]
      }
}
