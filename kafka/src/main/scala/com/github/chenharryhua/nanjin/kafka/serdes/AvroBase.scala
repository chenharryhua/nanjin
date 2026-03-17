package com.github.chenharryhua.nanjin.kafka.serdes

import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Json, Printer}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

sealed trait AvroBase[A] extends Unregistered[A]

object AvroBase {
  def apply[A](using ev: AvroBase[A]): AvroBase[A] = ev

  given [A: {Encoder, Decoder, SchemaFor}] => AvroBase[A] = new AvroBase[A] {
    private val schema = SchemaFor[A].schema
    private val dec = FromRecord[A](schema)
    private val enc = ToRecord[A](schema)

    override protected val unregistered: Serde[A] =
      new Serde[A] {
        /*
         * Serializer
         */
        override val serializer: Serializer[A] =
          new Serializer[A] {
            private val ser = new GenericAvroSerializer

            override def serialize(topic: String, data: A): Array[Byte] =
              ser.serialize(topic, enc.to(data))

            override def serialize(topic: String, headers: Headers, data: A): Array[Byte] =
              ser.serialize(topic, headers, enc.to(data))
          }
        /*
         * Deserializer
         */

        override val deserializer: Deserializer[A] =
          new Deserializer[A] {
            private val deSer = new GenericAvroDeserializer

            override def deserialize(topic: String, data: Array[Byte]): A =
              dec.from(deSer.deserialize(topic, data))

            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): A =
              dec.from(deSer.deserialize(topic, headers, data))
          }
      }
  }

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
        override val serializer: Serializer[GenericRecord] =
          new Serializer[GenericRecord] {
            private val ser = new GenericAvroSerializer
            export ser.*
          }
        /*
         * Deserializer
         */
        override val deserializer: Deserializer[GenericRecord] =
          new Deserializer[GenericRecord] {
            private val deSer = new GenericAvroDeserializer
            export deSer.*
          }
      }
  }
}
