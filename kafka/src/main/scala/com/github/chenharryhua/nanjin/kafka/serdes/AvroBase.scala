package com.github.chenharryhua.nanjin.kafka.serdes

import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import io.circe.{Json, Printer}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

sealed trait AvroBase[A] extends Unregistered[A]

object AvroBase {

  given AvroBase[Json] = new AvroBase[Json] {
    override protected val unregistered: Serde[Json] =
      new Serde[Json] {
        /*
         * Serializer
         */
        override val serializer: Serializer[Json] =
          new Serializer[Json] {
            private val ser = Serdes.ByteBuffer().serializer()
            private val print = Printer.noSpaces
            override def serialize(topic: String, data: Json): Array[Byte] =
              Option(data)
                .map(js => ser.serialize(topic, print.printToByteBuffer(js)))
                .orNull

            override def serialize(topic: String, headers: Headers, data: Json): Array[Byte] =
              Option(data)
                .map(js => ser.serialize(topic, headers, print.printToByteBuffer(js)))
                .orNull
          }
        /*
         * Deserializer
         */
        override val deserializer: Deserializer[Json] =
          new Deserializer[Json] {
            import io.circe.jawn.parseByteArray
            override def deserialize(topic: String, data: Array[Byte]): Json =
              Option(data).map { ab =>
                parseByteArray(ab) match {
                  case Left(value)  => throw value // scalafix:ok
                  case Right(value) => value
                }
              }.orNull

            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Json =
              deserialize(topic, data)
          }
      }
  }

  given AvroBase[GenericRecord] = new AvroBase[GenericRecord] {
    override protected val unregistered: Serde[GenericRecord] =
      new Serde[GenericRecord] {
        /*
         * Serializer
         */
        override def serializer: Serializer[GenericRecord] =
          new Serializer[GenericRecord] {
            private val ser = new GenericAvroSerializer
            export ser.*
          }
        /*
         * Deserializer
         */
        override def deserializer: Deserializer[GenericRecord] =
          new Deserializer[GenericRecord] {
            private val deSer = new GenericAvroDeserializer
            export deSer.*
          }
      }
  }
}
