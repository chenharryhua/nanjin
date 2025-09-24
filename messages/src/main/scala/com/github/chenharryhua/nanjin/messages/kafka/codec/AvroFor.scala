package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.{catsSyntaxOptionId, none}
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Printer}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import io.estatico.newtype.macros.newtype
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import java.util
import java.util.UUID

/*
 * spark friendly
 */
sealed trait AvroFor[A] extends RegisterSerde[A] {
  val schema: Option[Schema]
}

private[codec] trait LowerPriority {
  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder]: AvroFor[A] =
    AvroFor(AvroCodec[A])
}

object AvroFor extends LowerPriority {
  def apply[A](implicit ev: AvroFor[A]): AvroFor[A] = macro imp.summon[AvroFor[A]]

  @newtype final case class Universal(value: GenericRecord)
  object Universal {
    implicit val jsonEncoderUniversal: JsonEncoder[Universal] =
      (a: Universal) =>
        io.circe.jawn.parse(a.value.toString) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
  }

  /*
   * Specific
   */

// 1: String
  implicit object avroForString extends AvroFor[String] {
    override val schema: Option[Schema] = SchemaFor[String].schema.some
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  // 2: Long
  implicit object avroForLong extends AvroFor[Long] {
    override val schema: Option[Schema] = SchemaFor[Long].schema.some
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

// 3: array byte
  implicit object avroForArrayByte extends AvroFor[Array[Byte]] {
    override val schema: Option[Schema] = SchemaFor[Array[Byte]].schema.some
    override protected val unregisteredSerde: Serde[Array[Byte]] = serializable.byteArraySerde
  }

  // 4: byte buffer
  implicit object avroForByteBuffer extends AvroFor[ByteBuffer] {
    override val schema: Option[Schema] = SchemaFor[ByteBuffer].schema.some
    override protected val unregisteredSerde: Serde[ByteBuffer] = serializable.byteBufferSerde
  }

  // 5: short
  implicit object avroForShort extends AvroFor[Short] {
    override val schema: Option[Schema] = SchemaFor[Short].schema.some
    override protected val unregisteredSerde: Serde[Short] = serializable.shortSerde
  }

  // 6: float
  implicit object avroForFloat extends AvroFor[Float] {
    override val schema: Option[Schema] = SchemaFor[Float].schema.some
    override protected val unregisteredSerde: Serde[Float] = serializable.floatSerde
  }

  // 7: double
  implicit object avroForDouble extends AvroFor[Double] {
    override val schema: Option[Schema] = SchemaFor[Double].schema.some
    override protected val unregisteredSerde: Serde[Double] = serializable.doubleSerde
  }

  // 8: int
  implicit object avroForInt extends AvroFor[Int] {
    override val schema: Option[Schema] = SchemaFor[Int].schema.some
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  // 9: uuid
  implicit object avroForUUID extends AvroFor[UUID] {
    override val schema: Option[Schema] = SchemaFor[UUID].schema.some
    override protected val unregisteredSerde: Serde[UUID] = serializable.uuidSerde
  }

  // 10. universal - generic record
  implicit object avroForUniversal extends AvroFor[Universal] {

    override val schema: Option[Schema] = none[Schema]

    override protected val unregisteredSerde: Serde[Universal] =
      new Serde[Universal] with Serializable {
        override val serializer: Serializer[Universal] =
          new Serializer[Universal] with Serializable {
            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = ()
            override def close(): Unit = ()
            override def serialize(topic: String, data: Universal): Array[Byte] =
              throw ForbiddenProduceException("Avro")
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {

            @transient private[this] lazy val deSer = new GenericAvroDeserializer

            override def close(): Unit = deSer.close()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def deserialize(topic: String, data: Array[Byte]): Universal =
              Universal(deSer.deserialize(topic, data))
          }
      }
  }

  /*
   * General
   */

  def apply[A](codec: AvroCodec[A]): AvroFor[A] =
    new AvroFor[A] {
      override val schema: Option[Schema] = codec.schema.some

      override protected val unregisteredSerde: Serde[A] = new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
          new Serializer[A] with Serializable {
            @transient private[this] lazy val ser = new GenericAvroSerializer
            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            override def serialize(topic: String, data: A): Array[Byte] =
              if (data == null) null
              else
                codec.encode(data) match {
                  case gr: GenericRecord => ser.serialize(topic, gr)

                  case ex => sys.error(s"${ex.getClass.toString} is not Generic Record")
                }
          }

        override val deserializer: Deserializer[A] =
          new Deserializer[A] with Serializable {

            @transient private[this] lazy val deSer = new GenericAvroDeserializer

            override def close(): Unit = deSer.close()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            @SuppressWarnings(Array("AsInstanceOf"))
            override def deserialize(topic: String, data: Array[Byte]): A =
              if (data == null) null.asInstanceOf[A]
              else codec.decode(deSer.deserialize(topic, data))
          }
      }
    }

  implicit def avroForKJson[A: JsonEncoder: JsonDecoder]: AvroFor[KJson[A]] =
    new AvroFor[KJson[A]] {
      override val schema: Option[Schema] = Some(SchemaFor[String].schema)

      private val serdes_internal: Serde[ByteBuffer] = Serdes.byteBufferSerde

      override protected val unregisteredSerde: Serde[KJson[A]] =
        new Serde[KJson[A]] with Serializable {
          override val serializer: Serializer[KJson[A]] =
            new Serializer[KJson[A]] with Serializable {
              private val ser = serdes_internal.serializer()
              private val print = Printer.noSpaces
              override def serialize(topic: String, data: KJson[A]): Array[Byte] =
                ser.serialize(topic, print.printToByteBuffer(data.value.asJson))
            }

          override val deserializer: Deserializer[KJson[A]] =
            new Deserializer[KJson[A]] with Serializable {
              private val deSer = serdes_internal.deserializer()
              override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
                if (data == null) null
                else
                  io.circe.jawn.decodeByteBuffer[A](deSer.deserialize(topic, data)) match {
                    case Left(value)  => throw value
                    case Right(value) => KJson(value)
                  }
            }
        }
    }
}
