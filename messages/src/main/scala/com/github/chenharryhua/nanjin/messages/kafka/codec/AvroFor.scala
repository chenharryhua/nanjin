package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.{catsSyntaxOptionId, none}
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Codec as JsonCodec, Decoder as JsonDecoder, Encoder as JsonEncoder, HCursor, Json, Printer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops.toCoercibleIdOps
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import java.util
import java.util.UUID

sealed trait AvroFor[A] extends RegisterSerde[A] {
  val schema: Option[AvroSchema]
}

private[codec] trait LowerPriority {
  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder]: AvroFor[A] =
    AvroFor(AvroCodec[A])
}

object AvroFor extends LowerPriority {
  def apply[A](implicit ev: AvroFor[A]): AvroFor[A] = ev

  @newtype final class FromBroker private (val value: GenericRecord)
  protected object FromBroker {
    implicit val jsonEncoderUniversal: JsonEncoder[FromBroker] =
      (a: FromBroker) =>
        io.circe.jawn.parse(a.value.toString) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
  }

  @newtype final class KJson[A] private (val value: A)
  object KJson {
    def apply[A](a: A): KJson[A] = a.coerce[KJson[A]]

    implicit def jsonCodec[A: JsonEncoder: JsonDecoder]: JsonCodec[KJson[A]] =
      new JsonCodec[KJson[A]] {
        override def apply(a: KJson[A]): Json = JsonEncoder[A].apply(a.value)
        override def apply(c: HCursor): Result[KJson[A]] = JsonDecoder[A].apply(c).map(KJson[A])
      }
  }

  /*
   * Specific
   */

// 1: String
  implicit object avroForString extends AvroFor[String] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[String].schema).some
    override protected val unregisteredSerde: Serde[String] = Serdes.stringSerde
  }

  // 2: Long
  implicit object avroForLong extends AvroFor[Long] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Long].schema).some
    override protected val unregisteredSerde: Serde[Long] = Serdes.longSerde
  }

// 3: array byte
  implicit object avroForArrayByte extends AvroFor[Array[Byte]] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Array[Byte]].schema).some
    override protected val unregisteredSerde: Serde[Array[Byte]] = Serdes.byteArraySerde
  }

  // 4: byte buffer
  implicit object avroForByteBuffer extends AvroFor[ByteBuffer] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[ByteBuffer].schema).some
    override protected val unregisteredSerde: Serde[ByteBuffer] = Serdes.byteBufferSerde
  }

  // 5: short
  implicit object avroForShort extends AvroFor[Short] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Short].schema).some
    override protected val unregisteredSerde: Serde[Short] = Serdes.shortSerde
  }

  // 6: float
  implicit object avroForFloat extends AvroFor[Float] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Float].schema).some
    override protected val unregisteredSerde: Serde[Float] = Serdes.floatSerde
  }

  // 7: double
  implicit object avroForDouble extends AvroFor[Double] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Double].schema).some
    override protected val unregisteredSerde: Serde[Double] = Serdes.doubleSerde
  }

  // 8: int
  implicit object avroForInt extends AvroFor[Int] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[Int].schema).some
    override protected val unregisteredSerde: Serde[Int] = Serdes.intSerde
  }

  // 9: uuid
  implicit object avroForUUID extends AvroFor[UUID] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[UUID].schema).some
    override protected val unregisteredSerde: Serde[UUID] = Serdes.uuidSerde
  }

  // 10. universal - generic record
  implicit object avroForFromBroker extends AvroFor[FromBroker] {

    override val schema: Option[AvroSchema] = none[AvroSchema]

    override protected val unregisteredSerde: Serde[FromBroker] = new Serde[FromBroker] {
      override val serializer: Serializer[FromBroker] =
        new Serializer[FromBroker] {
          private[this] val ser = new GenericAvroSerializer

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: FromBroker): Array[Byte] =
            Option(data).flatMap(u => Option(u.value)).map(gr => ser.serialize(topic, gr)).orNull
        }

      override val deserializer: Deserializer[FromBroker] = new Deserializer[FromBroker] {
        private[this] val deSer = new GenericAvroDeserializer

        override def close(): Unit = deSer.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): FromBroker =
          Option(deSer.deserialize(topic, data))
            .map(_.coerce[FromBroker])
            .getOrElse(null.asInstanceOf[FromBroker])
      }
    }
  }

  /*
   * General
   */

  def apply[A](codec: AvroCodec[A]): AvroFor[A] = new AvroFor[A] {
    override val schema: Option[AvroSchema] = new AvroSchema(codec.schema).some

    override protected val unregisteredSerde: Serde[A] = new Serde[A] {
      override val serializer: Serializer[A] = new Serializer[A] {
        private[this] val ser = new GenericAvroSerializer
        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: A): Array[Byte] =
          Option(data).map(a => ser.serialize(topic, codec.toRecord(a))).orNull
      }

      override val deserializer: Deserializer[A] = new Deserializer[A] {
        private[this] val deSer = new GenericAvroDeserializer
        override def close(): Unit = deSer.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): A =
          Option(deSer.deserialize(topic, data)).map(codec.fromRecord).getOrElse(null.asInstanceOf[A])
      }
    }
  }

  implicit def avroForKJson[A: JsonEncoder: JsonDecoder]: AvroFor[KJson[A]] = new AvroFor[KJson[A]] {
    override val schema: Option[AvroSchema] = new AvroSchema(SchemaFor[String].schema).some

    override protected val unregisteredSerde: Serde[KJson[A]] = new Serde[KJson[A]] {
      override val serializer: Serializer[KJson[A]] = new Serializer[KJson[A]] {
        private[this] val ser = Serdes.byteBufferSerde.serializer()
        private val print = Printer.noSpaces
        override def serialize(topic: String, data: KJson[A]): Array[Byte] =
          Option(data)
            .flatMap(k => Option(k.value))
            .map(js => ser.serialize(topic, print.printToByteBuffer(js.asJson)))
            .orNull
      }

      override val deserializer: Deserializer[KJson[A]] = new Deserializer[KJson[A]] {
        override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
          Option(data).map { ab =>
            io.circe.jawn.decodeByteArray[A](ab) match {
              case Left(value)  => throw value
              case Right(value) => KJson(value)
            }
          }.getOrElse(null.asInstanceOf[KJson[A]])
      }
    }
  }
}
