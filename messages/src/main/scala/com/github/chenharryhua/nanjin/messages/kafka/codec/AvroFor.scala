package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.{catsSyntaxOptionId, none}
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Codec as JsonCodec, Decoder as JsonDecoder, Encoder as JsonEncoder, HCursor, Json, Printer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import shapeless.LabelledGeneric

import java.util

sealed trait AvroFor[A] extends UnregisteredSerde[A] {
  val avroSchema: Option[AvroSchema]
}

sealed trait LowerPriority {
  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder: LabelledGeneric]: AvroFor[A] =
    AvroFor(AvroCodec[A])
}

object AvroFor extends LowerPriority {
  def apply[A](implicit ev: AvroFor[A]): AvroFor[A] = ev

  final class FromBroker private[AvroFor] (val value: GenericRecord)
  object FromBroker {
    implicit val jsonEncoderUniversal: JsonEncoder[FromBroker] =
      (a: FromBroker) =>
        io.circe.jawn.parse(a.value.toString) match {
          case Left(value)  => throw value // scalafix:ok
          case Right(value) => value
        }
  }

  final class KJson[A] private (val value: A)
  object KJson {
    def apply[A](a: A): KJson[A] = new KJson[A](a)
    implicit def jsonCodec[A: JsonEncoder: JsonDecoder]: JsonCodec[KJson[A]] =
      new JsonCodec[KJson[A]] {
        override def apply(a: KJson[A]): Json = JsonEncoder[A].apply(a.value)
        override def apply(c: HCursor): Result[KJson[A]] = JsonDecoder[A].apply(c).map(new KJson[A](_))
      }
  }

  /*
   * Specific
   */

  // 1: array byte
  implicit object avroForArrayByte extends AvroFor[Array[Byte]] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaFor[Array[Byte]].schema).some
    override protected val unregisteredSerde: Serde[Array[Byte]] = Serdes.ByteArray()
  }

  // 2: String
  implicit object avroForString extends AvroFor[String] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaBuilder.builder().stringType()).some
    override protected val unregisteredSerde: Serde[String] = Serdes.String()
  }

  // 3: Long
  implicit object avroForLong extends AvroFor[java.lang.Long] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaBuilder.builder().longType()).some
    override protected val unregisteredSerde: Serde[java.lang.Long] = Serdes.Long()
  }

  // 4: float
  implicit object avroForFloat extends AvroFor[java.lang.Float] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaBuilder.builder().floatType()).some
    override protected val unregisteredSerde: Serde[java.lang.Float] = Serdes.Float()
  }

  // 5: double
  implicit object avroForDouble extends AvroFor[java.lang.Double] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaBuilder.builder().doubleType()).some
    override protected val unregisteredSerde: Serde[java.lang.Double] = Serdes.Double()
  }

  // 6: int
  implicit object avroForInt extends AvroFor[java.lang.Integer] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaBuilder.builder().intType()).some
    override protected val unregisteredSerde: Serde[java.lang.Integer] = Serdes.Integer()
  }

  // 7. universal - generic record
  implicit object avroForFromBroker extends AvroFor[FromBroker] {
    override val isPrimitive: Boolean = false
    override val avroSchema: Option[AvroSchema] = none[AvroSchema]

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
            .map(new FromBroker(_))
            .orNull

      }
    }
  }

  /*
   * General
   */

  def apply[A](codec: AvroCodec[A]): AvroFor[A] = new AvroFor[A] {
    override val isPrimitive: Boolean = false
    override val avroSchema: Option[AvroSchema] = new AvroSchema(codec.schema).some

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
          Option(deSer.deserialize(topic, data))
            .map(codec.fromRecord)
            .getOrElse(null.asInstanceOf[A]) // scalafix:ok

      }
    }
  }

  implicit def avroForKJson[A: JsonEncoder: JsonDecoder]: AvroFor[KJson[A]] = new AvroFor[KJson[A]] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaFor[String].schema).some

    override protected val unregisteredSerde: Serde[KJson[A]] = new Serde[KJson[A]] {
      override val serializer: Serializer[KJson[A]] = new Serializer[KJson[A]] {
        private[this] val ser = Serdes.ByteBuffer().serializer()
        private val print = Printer.noSpaces
        override def serialize(topic: String, data: KJson[A]): Array[Byte] =
          Option(data)
            .flatMap(k => Option(k.value))
            .map(js => ser.serialize(topic, print.printToByteBuffer(js.asJson)))
            .orNull
      }

      override val deserializer: Deserializer[KJson[A]] = new Deserializer[KJson[A]] {
        import io.circe.jawn.decodeByteArray
        override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
          Option(data).map { ab =>
            decodeByteArray[A](ab) match {
              case Left(value)  => throw value // scalafix:ok
              case Right(value) => KJson(value)
            }
          }.orNull
      }
    }
  }
}
