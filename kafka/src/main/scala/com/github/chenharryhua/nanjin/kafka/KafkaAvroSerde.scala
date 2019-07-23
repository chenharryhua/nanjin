package com.github.chenharryhua.nanjin.kafka
import java.io.ByteArrayOutputStream

import cats.Show
import cats.implicits._
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

final case class KafkaAvro[A](value: A) extends AnyVal

object KafkaAvro {
  implicit def showKafkaAvro[A: Show]: Show[KafkaAvro[A]] = _.value.show
}

@SuppressWarnings(Array("AsInstanceOf"))
final class KafkaAvroSerde[A >: Null: SchemaFor: Encoder: Decoder] extends Serde[KafkaAvro[A]] {
  private[this] val schema: Schema = AvroSchema[A]

  override val serializer: Serializer[KafkaAvro[A]] =
    (topic: String, data: KafkaAvro[A]) =>
      Option(data).flatMap(x => Option(x.value)) match {
        case Some(d) =>
          val baos   = new ByteArrayOutputStream()
          val output = AvroOutputStream.binary[A].to(baos).build(schema)
          output.write(d)
          output.close()
          baos.toByteArray
        case None => null.asInstanceOf[Array[Byte]]
      }

  override val deserializer: Deserializer[KafkaAvro[A]] =
    (topic: String, data: Array[Byte]) =>
      Option(data) match {
        case Some(d) =>
          val input = AvroInputStream.binary[A].from(d).build(schema)
          KafkaAvro(input.iterator.next)
        case None => null.asInstanceOf[KafkaAvro[A]]
      }
}
