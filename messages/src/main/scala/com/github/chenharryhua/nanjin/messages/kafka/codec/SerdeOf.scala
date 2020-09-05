package com.github.chenharryhua.nanjin.messages.kafka.codec

import java.util

import com.github.chenharryhua.nanjin.messages.kafka.KeyValueTag
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import monocle.Prism
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

final class NJCodec[A](val topicName: String, val cfg: NJSerdeConfig[A]) extends Serializable {
  def encode(a: A): Array[Byte]  = cfg.serde.serializer.serialize(topicName, a)
  def decode(ab: Array[Byte]): A = cfg.serde.deserializer.deserialize(topicName, ab)

  def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(new NullPointerException))(x => Try(decode(x)))

  val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

sealed abstract class NJSerdeConfig[A](
  val tag: KeyValueTag,
  val serde: SerdeOf[A],
  val configProps: Map[String, String])
    extends Serializable {

  serde.serializer.configure(configProps.asJava, tag.isKey)
  serde.deserializer.configure(configProps.asJava, tag.isKey)

  final def codec(topicName: String): NJCodec[A] = new NJCodec[A](topicName, this)
}

trait SerdeOf[A] extends Serde[A] with Serializable {
  val avroCodec: AvroCodec[A]

  final def asKey(props: Map[String, String]): NJSerdeConfig[A] =
    new NJSerdeConfig(KeyValueTag.Key, this, props) {}

  final def asValue(props: Map[String, String]): NJSerdeConfig[A] =
    new NJSerdeConfig(KeyValueTag.Value, this, props) {}
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

          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

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

          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

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
          Serdes.Integer.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Int] =
      new Deserializer[Int] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Int =
          Serdes.Integer.deserializer.deserialize(topic, data)
      }
  }

  implicit object LongPrimitiveSerde extends SerdeOf[Long] {

    override val avroCodec: AvroCodec[Long] = AvroCodec[Long]

    override val serializer: Serializer[Long] =
      new Serializer[Long] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Long): Array[Byte] =
          Serdes.Long.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Long] =
      new Deserializer[Long] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Long =
          Serdes.Long.deserializer.deserialize(topic, data)
      }
  }

  implicit object StringPrimitiveSerde extends SerdeOf[String] {

    override val avroCodec: AvroCodec[String] = AvroCodec[String]

    override val serializer: Serializer[String] =
      new Serializer[String] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: String): Array[Byte] =
          Serdes.String.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[String] =
      new Deserializer[String] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): String =
          Serdes.String.deserializer.deserialize(topic, data)
      }
  }

  implicit object DoublePrimitiveSerde extends SerdeOf[Double] {

    override val avroCodec: AvroCodec[Double] = AvroCodec[Double]

    override val serializer: Serializer[Double] =
      new Serializer[Double] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Double): Array[Byte] =
          Serdes.Double.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Double] =
      new Deserializer[Double] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Double =
          Serdes.Double.deserializer.deserialize(topic, data)
      }
  }

  implicit object FloatPrimitiveSerde extends SerdeOf[Float] {

    override val avroCodec: AvroCodec[Float] = AvroCodec[Float]

    override val serializer: Serializer[Float] =
      new Serializer[Float] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Float): Array[Byte] =
          Serdes.Float.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Float] =
      new Deserializer[Float] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Float =
          Serdes.Float.deserializer.deserialize(topic, data)
      }
  }

  implicit object ByteArrayPrimitiveSerde extends SerdeOf[Array[Byte]] {

    override val avroCodec: AvroCodec[Array[Byte]] = AvroCodec[Array[Byte]]

    override val serializer: Serializer[Array[Byte]] =
      new Serializer[Array[Byte]] with Serializable {
        override def close(): Unit = ()

        override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
          Serdes.ByteArray.serializer.serialize(topic, data)
      }

    override val deserializer: Deserializer[Array[Byte]] =
      new Deserializer[Array[Byte]] with Serializable {
        override def close(): Unit = ()

        override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
          Serdes.ByteArray.deserializer.deserialize(topic, data)
      }
  }
}
