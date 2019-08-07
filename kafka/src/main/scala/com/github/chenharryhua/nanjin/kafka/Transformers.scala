package com.github.chenharryhua.nanjin.kafka

import com.sksamuel.avro4s._
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import monocle.{Iso, Prism}
import org.apache.avro.generic.GenericRecord

import scala.util.Try

object Transformers {

  def codec(topic: String, serde: GenericAvroSerde): Iso[Array[Byte], GenericRecord] =
    Iso[Array[Byte], GenericRecord](ab => serde.deserializer.deserialize(topic, ab))(rg =>
      serde.serializer.serialize(topic, rg))

  def transform[A](format: RecordFormat[A]): Iso[GenericRecord, A] = {
    Iso[GenericRecord, A](gr => format.from(gr))(a => format.to(a))
  }
}
