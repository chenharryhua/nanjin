package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.github.chenharryhua.nanjin.messages.kafka.KeyValueTag
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util
import scala.collection.JavaConverters.*
import scala.util.{Failure, Try}

/** [[https://github.com/sksamuel/avro4s]]
  */

/** @param name
  *   - topic name or state store name
  * @param registered
  *   registered in kafka schema registry
  * @tparam A
  *   schema related type
  */
final class NJCodec[A](val name: String, val registered: RegisteredSerde[A]) extends Serializable {
  def encode(a: A): Array[Byte]  = registered.serde.serializer.serialize(name, a)
  def decode(ab: Array[Byte]): A = registered.serde.deserializer.deserialize(name, ab)

  def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(new NullPointerException("NJCodec.tryDecode a null Array[Byte]")))(x =>
      Try(decode(x)))
}

sealed abstract class RegisteredSerde[A](
  val tag: KeyValueTag,
  val serde: SerdeOf[A],
  val configProps: Map[String, String])
    extends Serializable {

  serde.serializer.configure(configProps.asJava, tag.isKey)
  serde.deserializer.configure(configProps.asJava, tag.isKey)

  final def codec(topicName: String): NJCodec[A] = new NJCodec[A](topicName, this)
}

trait SerdeOf[A] extends Serde[A] with Serializable {
  def avroCodec: AvroCodec[A]

  final def asKey(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(KeyValueTag.Key, this, props) {}

  final def asValue(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(KeyValueTag.Value, this, props) {}
}

private[codec] trait LowerPriority {

  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder]: SerdeOf[A] =
    SerdeOf(AvroCodec[A])
}

object SerdeOf extends LowerPriority {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  def apply[A](codec: AvroCodec[A]): SerdeOf[A] =
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
                avroCodec.avroEncoder.encode(value) match {
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
              case Some(value) => avroCodec.avroDecoder.decode(deSer.deserialize(topic, value))
            }
        }
      override val avroCodec: AvroCodec[A] = codec
    }

  implicit object IntPrimitiveSerde extends SerdeOf[Int] {

    override val avroCodec: AvroCodec[Int] = AvroCodec[Int]

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

    override val avroCodec: AvroCodec[Long] = AvroCodec[Long]

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

    override val avroCodec: AvroCodec[String] = AvroCodec[String]

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

    override val avroCodec: AvroCodec[Double] = AvroCodec[Double]

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

    override val avroCodec: AvroCodec[Float] = AvroCodec[Float]

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

    override val avroCodec: AvroCodec[Array[Byte]] = AvroCodec[Array[Byte]]

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
