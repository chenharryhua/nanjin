package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.github.chenharryhua.nanjin.messages.kafka.KeyValueTag
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Try}

/** [[https://github.com/sksamuel/avro4s]]
  */

/** @param name
  *   - topic name or state store name
  * @param registered
  *   serializer/deserializer config method was called
  * @tparam A
  *   schema related type
  */
final class KafkaSerde[A](val name: String, registered: RegisteredSerde[A]) extends Serializable {
  val serde: Serde[A]                 = registered.serde
  def serialize(a: A): Array[Byte]    = serde.serializer.serialize(name, a)
  def deserialize(ab: Array[Byte]): A = serde.deserializer.deserialize(name, ab)

  def tryDeserialize(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(new NullPointerException("NJCodec.tryDecode a null Array[Byte]")))(x =>
      Try(deserialize(x)))
}

sealed abstract class RegisteredSerde[A](
  val tag: KeyValueTag,
  val configProps: Map[String, String],
  serdeOf: SerdeOf[A])
    extends Serializable {

  serdeOf.serializer.configure(configProps.asJava, tag.isKey)
  serdeOf.deserializer.configure(configProps.asJava, tag.isKey)

  lazy val serde: Serde[A] = serdeOf

  final def topic(topicName: String): KafkaSerde[A] =
    new KafkaSerde[A](topicName, this)
}

trait SerdeOf[A] extends Serde[A] with Serializable { outer =>
  def avroCodec: NJAvroCodec[A]

  final def asKey(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(KeyValueTag.Key, props, this) {}

  final def asValue(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(KeyValueTag.Value, props, this) {}

  final def withSchema(schema: Schema): SerdeOf[A] = new SerdeOf[A] {
    override def avroCodec: NJAvroCodec[A]     = outer.avroCodec.withSchema(schema)
    override def serializer: Serializer[A]     = outer.serializer()
    override def deserializer: Deserializer[A] = outer.deserializer()
  }
}

private[codec] trait LowerPriority {

  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder]: SerdeOf[A] =
    SerdeOf(NJAvroCodec[A])
}

object SerdeOf extends LowerPriority {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  def apply[A](codec: NJAvroCodec[A]): SerdeOf[A] =
    new SerdeOf[A] {

      override val serializer: Serializer[A] =
        new Serializer[A] with Serializable {
          @transient private[this] lazy val ser: GenericAvroSerializer = new GenericAvroSerializer

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          @SuppressWarnings(Array("AsInstanceOf"))
          override def serialize(topic: String, data: A): Array[Byte] =
            Option(data) match {
              case None => null.asInstanceOf[Array[Byte]]
              case Some(value) =>
                avroCodec.encode(value) match {
                  case gr: GenericRecord => ser.serialize(topic, gr)
                  case ex                => sys.error(s"not a generic record: ${ex.toString}")
                }
            }
        }

      override val deserializer: Deserializer[A] =
        new Deserializer[A] with Serializable {

          @transient private[this] lazy val deSer: GenericAvroDeserializer =
            new GenericAvroDeserializer

          override def close(): Unit =
            deSer.close()

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          @SuppressWarnings(Array("AsInstanceOf"))
          override def deserialize(topic: String, data: Array[Byte]): A =
            Option(data) match {
              case None        => null.asInstanceOf[A]
              case Some(value) => avroCodec.decode(deSer.deserialize(topic, value))
            }
        }
      override val avroCodec: NJAvroCodec[A] = codec
    }

  implicit object IntPrimitiveSerde extends SerdeOf[Int] {

    override val avroCodec: NJAvroCodec[Int] = NJAvroCodec[Int]

    override val serializer: Serializer[Int] =
      new Serializer[Int] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Int): Array[Byte] =
          Serdes.intSerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Int] =
      new Deserializer[Int] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Int =
          Serdes.intSerde.deserializer.deserialize(topic, data)
      }
  }

  implicit object LongPrimitiveSerde extends SerdeOf[Long] {

    override val avroCodec: NJAvroCodec[Long] = NJAvroCodec[Long]

    override val serializer: Serializer[Long] =
      new Serializer[Long] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Long): Array[Byte] =
          Serdes.longSerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Long] =
      new Deserializer[Long] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Long =
          Serdes.longSerde.deserializer.deserialize(topic, data)
      }
  }

  implicit object StringPrimitiveSerde extends SerdeOf[String] {

    override val avroCodec: NJAvroCodec[String] = NJAvroCodec[String]

    override val serializer: Serializer[String] =
      new Serializer[String] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: String): Array[Byte] =
          Serdes.stringSerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[String] =
      new Deserializer[String] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): String =
          Serdes.stringSerde.deserializer.deserialize(topic, data)
      }
  }

  implicit object DoublePrimitiveSerde extends SerdeOf[Double] {

    override val avroCodec: NJAvroCodec[Double] = NJAvroCodec[Double]

    override val serializer: Serializer[Double] =
      new Serializer[Double] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Double): Array[Byte] =
          Serdes.doubleSerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Double] =
      new Deserializer[Double] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Double =
          Serdes.doubleSerde.deserializer.deserialize(topic, data)
      }
  }

  implicit object FloatPrimitiveSerde extends SerdeOf[Float] {

    override val avroCodec: NJAvroCodec[Float] = NJAvroCodec[Float]

    override val serializer: Serializer[Float] =
      new Serializer[Float] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Float): Array[Byte] =
          Serdes.floatSerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Float] =
      new Deserializer[Float] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Float =
          Serdes.floatSerde.deserializer.deserialize(topic, data)
      }
  }

  implicit object ByteArrayPrimitiveSerde extends SerdeOf[Array[Byte]] {

    override val avroCodec: NJAvroCodec[Array[Byte]] = NJAvroCodec[Array[Byte]]

    override val serializer: Serializer[Array[Byte]] =
      new Serializer[Array[Byte]] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
          Serdes.byteArraySerde.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Array[Byte]] =
      new Deserializer[Array[Byte]] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
          Serdes.byteArraySerde.deserializer.deserialize(topic, data)
      }
  }
}
