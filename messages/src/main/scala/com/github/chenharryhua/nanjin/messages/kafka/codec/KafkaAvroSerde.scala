package com.github.chenharryhua.nanjin.messages.kafka.codec

import java.util

import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.Serdes

trait KafkaAvroSerializer[A] extends Serializer[A] with Serializable {
  val avroEncoder: AvroEncoder[A]
  override def serialize(topic: String, data: A): Array[Byte]
}

object KafkaAvroSerializer {
  def apply[A](implicit ev: KafkaAvroSerializer[A]): KafkaAvroSerializer[A] = ev

  def apply[A](encoder: AvroEncoder[A], schemaFor: SchemaFor[A]): KafkaAvroSerializer[A] =
    new KafkaAvroSerializer[A] {
      @transient private[this] lazy val ser: GenericAvroSerializer = new GenericAvroSerializer

      override val avroEncoder: AvroEncoder[A] =
        encoder.withSchema(schemaFor).resolveEncoder()

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override def serialize(topic: String, a: A): Array[Byte] =
        Option(a) match {
          case None => null.asInstanceOf[Array[Byte]]
          case Some(value) =>
            avroEncoder.encode(value) match {
              case gr: GenericRecord => ser.serialize(topic, gr)
              case ex                => sys.error(s"not a generic record: ${ex.toString}")
            }
        }
    }

  def apply[A](encoder: AvroEncoder[A]): KafkaAvroSerializer[A] =
    apply(encoder, encoder.schemaFor)

  implicit object IntPrimitiveSerializer extends KafkaAvroSerializer[Int] {
    override val avroEncoder: AvroEncoder[Int] = AvroEncoder[Int]

    override def serialize(topic: String, data: Int): Array[Byte] =
      Serdes.Integer.serializer.serialize(topic, data)
  }

  implicit object LongPrimitiveSerializer extends KafkaAvroSerializer[Long] {
    override val avroEncoder: AvroEncoder[Long] = AvroEncoder[Long]

    override def serialize(topic: String, data: Long): Array[Byte] =
      Serdes.Long.serializer.serialize(topic, data)
  }

  implicit object StringPrimitiveSerializer extends KafkaAvroSerializer[String] {
    override val avroEncoder: AvroEncoder[String] = AvroEncoder[String]

    override def serialize(topic: String, data: String): Array[Byte] =
      Serdes.String.serializer.serialize(topic, data)
  }

  implicit object DoublePrimitiveSerializer extends KafkaAvroSerializer[Double] {
    override val avroEncoder: AvroEncoder[Double] = AvroEncoder[Double]

    override def serialize(topic: String, data: Double): Array[Byte] =
      Serdes.Double.serializer.serialize(topic, data)
  }

  implicit object FloatPrimitiveSerializer extends KafkaAvroSerializer[Float] {

    override val avroEncoder: AvroEncoder[Float] = AvroEncoder[Float]

    override def serialize(topic: String, data: Float): Array[Byte] =
      Serdes.Float.serializer.serialize(topic, data)
  }

  implicit object ByteArrayPrimitiveSerializer extends KafkaAvroSerializer[Array[Byte]] {

    override val avroEncoder: AvroEncoder[Array[Byte]] = AvroEncoder[Array[Byte]]

    override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.serializer.serialize(topic, data)
  }
}

trait KafkaAvroDeserializer[A] extends Deserializer[A] with Serializable {
  val avroDecoder: AvroDecoder[A]
  override def deserialize(topic: String, data: Array[Byte]): A
}

object KafkaAvroDeserializer {
  def apply[A](implicit ev: KafkaAvroDeserializer[A]): KafkaAvroDeserializer[A] = ev

  def apply[A](decoder: AvroDecoder[A], schemaFor: SchemaFor[A]): KafkaAvroDeserializer[A] =
    new KafkaAvroDeserializer[A] {
      @transient private[this] lazy val deSer: GenericAvroDeserializer = new GenericAvroDeserializer

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override val avroDecoder: AvroDecoder[A] =
        decoder.withSchema(schemaFor).resolveDecoder()

      override def deserialize(topic: String, data: Array[Byte]): A =
        Option(data) match {
          case None        => null.asInstanceOf[A]
          case Some(value) => avroDecoder.decode(deSer.deserialize(topic, value))
        }
    }

  def apply[A](decoder: AvroDecoder[A]): KafkaAvroDeserializer[A] =
    apply[A](decoder, decoder.schemaFor)

  implicit object IntPrimitiveDeserializer extends KafkaAvroDeserializer[Int] {
    override val avroDecoder: AvroDecoder[Int] = AvroDecoder[Int]

    override def deserialize(topic: String, data: Array[Byte]): Int =
      Serdes.Integer.deserializer.deserialize(topic, data)
  }

  implicit object LongPrimitiveDeserializer extends KafkaAvroDeserializer[Long] {
    override val avroDecoder: AvroDecoder[Long] = AvroDecoder[Long]

    override def deserialize(topic: String, data: Array[Byte]): Long =
      Serdes.Long.deserializer.deserialize(topic, data)
  }

  implicit object StringPrimitiveDeserializer extends KafkaAvroDeserializer[String] {

    override val avroDecoder: AvroDecoder[String] = AvroDecoder[String]

    override def deserialize(topic: String, data: Array[Byte]): String =
      Serdes.String.deserializer.deserialize(topic, data)
  }

  implicit object DoublePrimitiveDeserializer extends KafkaAvroDeserializer[Double] {
    override val avroDecoder: AvroDecoder[Double] = AvroDecoder[Double]

    override def deserialize(topic: String, data: Array[Byte]): Double =
      Serdes.Double.deserializer.deserialize(topic, data)
  }

  implicit object FloatPrimitiveDeserializer extends KafkaAvroDeserializer[Float] {
    override val avroDecoder: AvroDecoder[Float] = AvroDecoder[Float]

    override def deserialize(topic: String, data: Array[Byte]): Float =
      Serdes.Float.deserializer.deserialize(topic, data)
  }

  implicit object ByteArrayPrimitiveDeserializer extends KafkaAvroDeserializer[Array[Byte]] {
    override val avroDecoder: AvroDecoder[Array[Byte]] = AvroDecoder[Array[Byte]]

    override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.deserializer.deserialize(topic, data)
  }
}
