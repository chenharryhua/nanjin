package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.{catsSyntaxOptionId, none}
import cats.kernel.Eq
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.circe.{Encoder as JsonEncoder, Json, Printer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import java.util

sealed trait AvroFor[A] extends UnregisteredSerde[A] {
  val avroSchema: Option[AvroSchema]
}

sealed trait LowerPriority {
  implicit def avro4sCodec[A: {SchemaFor, AvroEncoder, AvroDecoder}](implicit ev: Null <:< A): AvroFor[A] =
    AvroFor(AvroCodec[A])
}

object AvroFor extends LowerPriority {
  def apply[A](implicit ev: AvroFor[A]): AvroFor[A] = ev

  opaque type FromBroker = GenericRecord
  object FromBroker:
    private[AvroFor] def apply(gr: GenericRecord): FromBroker = gr
    extension (fb: FromBroker) def value: GenericRecord = fb
    given JsonEncoder[FromBroker] with
      override def apply(fb: FromBroker): Json =
        io.circe.jawn.parse(fb.value.toString) match {
          case Left(value)  => throw value // scalafix:ok
          case Right(value) => value
        }
    given Eq[FromBroker] = Eq.fromUniversalEquals

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
          private val ser = new GenericAvroSerializer

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: FromBroker): Array[Byte] =
            Option(data).flatMap(u => Option(u)).map(gr => ser.serialize(topic, gr)).orNull
        }

      override val deserializer: Deserializer[FromBroker] = new Deserializer[FromBroker] {
        private val deSer = new GenericAvroDeserializer

        override def close(): Unit = deSer.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): FromBroker =
          Option(deSer.deserialize(topic, data))
            .map(FromBroker(_))
            .orNull

      }
    }
  }

  /*
   * General
   */

  def apply[A](codec: AvroCodec[A])(implicit ev: Null <:< A): AvroFor[A] = new AvroFor[A] {
    override val isPrimitive: Boolean = false
    override val avroSchema: Option[AvroSchema] = new AvroSchema(codec.schemaFor.schema).some

    override protected val unregisteredSerde: Serde[A] = new Serde[A] {
      override val serializer: Serializer[A] = new Serializer[A] {
        private val ser = new GenericAvroSerializer
        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: A): Array[Byte] =
          Option(data).map(a => ser.serialize(topic, codec.toRecord(a))).orNull
      }

      override val deserializer: Deserializer[A] = new Deserializer[A] {
        private val deSer = new GenericAvroDeserializer
        override def close(): Unit = deSer.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): A =
          Option(deSer.deserialize(topic, data))
            .map(codec.fromRecord)
            .orNull

      }
    }
  }

  implicit def avroForKJson: AvroFor[Json] = new AvroFor[Json] {
    override val isPrimitive: Boolean = true
    override val avroSchema: Option[AvroSchema] = new AvroSchema(SchemaFor[String].schema).some

    override protected val unregisteredSerde: Serde[Json] = new Serde[Json] {
      override val serializer: Serializer[Json] = new Serializer[Json] {
        private val ser = Serdes.ByteBuffer().serializer()
        private val print = Printer.noSpaces
        override def serialize(topic: String, data: Json): Array[Byte] =
          Option(data)
            .map(js => ser.serialize(topic, print.printToByteBuffer(js)))
            .orNull
      }

      override val deserializer: Deserializer[Json] = new Deserializer[Json] {
        import io.circe.jawn.parseByteArray
        override def deserialize(topic: String, data: Array[Byte]): Json =
          Option(data).map { ab =>
            parseByteArray(ab) match {
              case Left(value)  => throw value // scalafix:ok
              case Right(value) => value
            }
          }.orNull
      }
    }
  }
}
