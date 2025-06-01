package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Try}

/** [[https://github.com/sksamuel/avro4s]]
  */

/** @param topicName
  *   - topic name or state store name
  * @param registered
  *   serializer/deserializer config method was called
  * @tparam A
  *   schema related type
  */
final class KafkaSerde[A](val topicName: TopicName, registered: RegisteredSerde[A]) extends Serializable {
  val serde: Serde[A]                 = registered.serde
  def serialize(a: A): Array[Byte]    = serde.serializer.serialize(topicName.value, a)
  def deserialize(ab: Array[Byte]): A = serde.deserializer.deserialize(topicName.value, ab)

  def tryDeserialize(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(new NullPointerException("NJCodec.tryDecode a null Array[Byte]")))(x =>
      Try(deserialize(x)))
}

sealed abstract class RegisteredSerde[A](serdeOf: SerdeOf[A]) extends Serializable {

  val serde: Serde[A] = serdeOf

  final def withTopic(topicName: TopicName): KafkaSerde[A] =
    new KafkaSerde[A](topicName, this)
}

trait SerdeOf[A] extends Serde[A] with Serializable { outer =>
  def avroCodec: AvroCodec[A]

  final def asKey(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(this) {
      serializer.configure(props.asJava, true)
      deserializer.configure(props.asJava, true)
    }

  final def asValue(props: Map[String, String]): RegisteredSerde[A] =
    new RegisteredSerde(this) {
      serializer.configure(props.asJava, false)
      deserializer.configure(props.asJava, false)
    }
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
              case None        => null.asInstanceOf[Array[Byte]]
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
      override val avroCodec: AvroCodec[A] = codec
    }

// 1: String
  implicit object StringPrimitiveSerde extends SerdeOf[String] {

    override val avroCodec: AvroCodec[String] = AvroCodec[String]

    override val serializer: Serializer[String] =
      new Serializer[String] with Serializable {
        private val delegate: Serializer[String] = Serdes.stringSerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: String): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[String] =
      new Deserializer[String] with Serializable {
        private val delegate: Deserializer[String] = Serdes.stringSerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): String =
          delegate.deserialize(topic, data)
      }
  }

  // 2: Long
  implicit object LongPrimitiveSerde extends SerdeOf[Long] {

    override val avroCodec: AvroCodec[Long] = AvroCodec[Long]

    override val serializer: Serializer[Long] =
      new Serializer[Long] with Serializable {
        private val delegate: Serializer[Long] = Serdes.longSerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Long): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Long] =
      new Deserializer[Long] with Serializable {
        private val delegate: Deserializer[Long] = Serdes.longSerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Long =
          delegate.deserialize(topic, data)
      }
  }

// 3: array byte
  implicit object ByteArrayPrimitiveSerde extends SerdeOf[Array[Byte]] {

    override val avroCodec: AvroCodec[Array[Byte]] = AvroCodec[Array[Byte]]

    override val serializer: Serializer[Array[Byte]] =
      new Serializer[Array[Byte]] with Serializable {
        private val delegate: Serializer[Array[Byte]] = Serdes.byteArraySerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Array[Byte]] =
      new Deserializer[Array[Byte]] with Serializable {
        private val delegate: Deserializer[Array[Byte]] = Serdes.byteArraySerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
          delegate.deserialize(topic, data)
      }
  }

  // 4: byte buffer
  implicit object ByteBufferPrimitiveSerde extends SerdeOf[ByteBuffer] {

    override val avroCodec: AvroCodec[ByteBuffer] = AvroCodec[ByteBuffer]

    override val serializer: Serializer[ByteBuffer] =
      new Serializer[ByteBuffer] with Serializable {
        private val delegate: Serializer[ByteBuffer] = Serdes.byteBufferSerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: ByteBuffer): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[ByteBuffer] =
      new Deserializer[ByteBuffer] with Serializable {
        private val delegate: Deserializer[ByteBuffer] = Serdes.byteBufferSerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): ByteBuffer =
          delegate.deserialize(topic, data)
      }
  }

  // 5: short
  implicit object ShortPrimitiveSerde extends SerdeOf[Short] {

    override val avroCodec: AvroCodec[Short] = AvroCodec[Short]

    override val serializer: Serializer[Short] =
      new Serializer[Short] with Serializable {
        private val delegate: Serializer[Short] = Serdes.shortSerde.serializer()
        override def close(): Unit              = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Short): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Short] =
      new Deserializer[Short] with Serializable {
        private val delegate: Deserializer[Short] = Serdes.shortSerde.deserializer()
        override def close(): Unit                = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Short =
          delegate.deserialize(topic, data)
      }
  }

  // 6: float
  implicit object FloatPrimitiveSerde extends SerdeOf[Float] {

    override val avroCodec: AvroCodec[Float] = AvroCodec[Float]

    override val serializer: Serializer[Float] =
      new Serializer[Float] with Serializable {
        private val delegate: Serializer[Float] = Serdes.floatSerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Float): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Float] =
      new Deserializer[Float] with Serializable {
        private val delegate: Deserializer[Float] = Serdes.floatSerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Float =
          delegate.deserialize(topic, data)
      }
  }

  // 7: double
  implicit object DoublePrimitiveSerde extends SerdeOf[Double] {

    override val avroCodec: AvroCodec[Double] = AvroCodec[Double]

    override val serializer: Serializer[Double] =
      new Serializer[Double] with Serializable {
        private val delegate: Serializer[Double] = Serdes.doubleSerde.serializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Double): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Double] =
      new Deserializer[Double] with Serializable {
        private val delegate: Deserializer[Double] = Serdes.doubleSerde.deserializer()

        override def close(): Unit = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Double =
          delegate.deserialize(topic, data)
      }
  }

  // 8: int
  implicit object IntPrimitiveSerde extends SerdeOf[Int] {

    override val avroCodec: AvroCodec[Int] = AvroCodec[Int]

    override val serializer: Serializer[Int] =
      new Serializer[Int] with Serializable {
        private val delegate: Serializer[Int] = Serdes.intSerde.serializer()
        override def close(): Unit            = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: Int): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[Int] =
      new Deserializer[Int] with Serializable {
        private val delegate: Deserializer[Int] = Serdes.intSerde.deserializer()
        override def close(): Unit              = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): Int =
          delegate.deserialize(topic, data)
      }
  }

  // 9: uuid
  implicit object UUIDPrimitiveSerde extends SerdeOf[UUID] {

    override val avroCodec: AvroCodec[UUID] = AvroCodec[UUID]

    override val serializer: Serializer[UUID] =
      new Serializer[UUID] with Serializable {
        private val delegate: Serializer[UUID] = Serdes.uuidSerde.serializer()
        override def close(): Unit             = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def serialize(topic: String, data: UUID): Array[Byte] =
          delegate.serialize(topic, data)
      }

    override val deserializer: Deserializer[UUID] =
      new Deserializer[UUID] with Serializable {
        private val delegate: Deserializer[UUID] = Serdes.uuidSerde.deserializer()
        override def close(): Unit               = delegate.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          delegate.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): UUID =
          delegate.deserialize(topic, data)
      }
  }
}
