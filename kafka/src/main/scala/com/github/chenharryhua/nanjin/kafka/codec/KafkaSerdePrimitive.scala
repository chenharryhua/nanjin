package com.github.chenharryhua.nanjin.kafka.codec

import com.sksamuel.avro4s.{Decoder, DefaultFieldMapper, Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.Serdes

sealed abstract private[codec] class KafkaPrimitiveSerializer[A](val schemaFor: SchemaFor[A])
    extends Serializer[A] with Serializable {
  val avroEncoder: Encoder[A]
  override def serialize(topic: String, data: A): Array[Byte]
}

object KafkaPrimitiveSerializer {

  implicit object IntKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Int](SchemaFor[Int]) {
    override val avroEncoder: Encoder[Int] = Encoder[Int]

    override def serialize(topic: String, data: Int): Array[Byte] =
      Serdes.Integer.serializer.serialize(topic, data)
  }

  implicit object LongKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Long](SchemaFor[Long]) {
    override val avroEncoder: Encoder[Long] = Encoder[Long]

    override def serialize(topic: String, data: Long): Array[Byte] =
      Serdes.Long.serializer.serialize(topic, data)
  }

  implicit object StringKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[String](SchemaFor[String]) {
    override val avroEncoder: Encoder[String] = Encoder[String]

    override def serialize(topic: String, data: String): Array[Byte] =
      Serdes.String.serializer.serialize(topic, data)
  }

  implicit object DoubleKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Double](SchemaFor[Double]) {
    override val avroEncoder: Encoder[Double] = Encoder[Double]

    override def serialize(topic: String, data: Double): Array[Byte] =
      Serdes.Double.serializer.serialize(topic, data)
  }

  implicit object FloatKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Float](SchemaFor[Float]) {

    override val avroEncoder: Encoder[Float] = Encoder[Float]

    override def serialize(topic: String, data: Float): Array[Byte] =
      Serdes.Float.serializer.serialize(topic, data)
  }

  implicit object ByteArrayKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Array[Byte]](SchemaFor[Array[Byte]]) {

    override val avroEncoder: Encoder[Array[Byte]] = Encoder[Array[Byte]]

    override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.serializer.serialize(topic, data)
  }
}

sealed abstract private[codec] class KafkaPrimitiveDeserializer[A]
    extends Deserializer[A] with Serializable {
  val avroDecoder: Decoder[A]
  override def deserialize(topic: String, data: Array[Byte]): A
}

object KafkaPrimitiveDeserializer {

  implicit object IntKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Int] {
    override val avroDecoder: Decoder[Int] = Decoder[Int]

    override def deserialize(topic: String, data: Array[Byte]): Int =
      Serdes.Integer.deserializer.deserialize(topic, data)
  }

  implicit object LongKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Long] {
    override val avroDecoder: Decoder[Long] = Decoder[Long]

    override def deserialize(topic: String, data: Array[Byte]): Long =
      Serdes.Long.deserializer.deserialize(topic, data)
  }

  implicit object StringKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[String] {

    override val avroDecoder: Decoder[String] = Decoder[String]

    override def deserialize(topic: String, data: Array[Byte]): String =
      Serdes.String.deserializer.deserialize(topic, data)
  }

  implicit object DoubleKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Double] {
    override val avroDecoder: Decoder[Double] = Decoder[Double]

    override def deserialize(topic: String, data: Array[Byte]): Double =
      Serdes.Double.deserializer.deserialize(topic, data)
  }

  implicit object FloatKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Float] {
    override val avroDecoder: Decoder[Float] = Decoder[Float]

    override def deserialize(topic: String, data: Array[Byte]): Float =
      Serdes.Float.deserializer.deserialize(topic, data)
  }

  implicit object ByteArrayKafkaPrimitiveDeserializer
      extends KafkaPrimitiveDeserializer[Array[Byte]] {
    override val avroDecoder: Decoder[Array[Byte]] = Decoder[Array[Byte]]

    override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.deserializer.deserialize(topic, data)
  }
}
