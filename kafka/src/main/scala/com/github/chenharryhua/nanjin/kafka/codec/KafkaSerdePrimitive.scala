package com.github.chenharryhua.nanjin.kafka.codec

import com.sksamuel.avro4s.{DefaultFieldMapper, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.Serdes

sealed abstract private[codec] class KafkaPrimitiveSerializer[A](val schema: Schema)
    extends Serializer[A] with Serializable {
  override def serialize(topic: String, data: A): Array[Byte]
}

object KafkaPrimitiveSerializer {

  implicit object IntKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Int](SchemaFor[Int].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: Int): Array[Byte] =
      Serdes.Integer.serializer.serialize(topic, data)
  }

  implicit object LongKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Long](SchemaFor[Long].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: Long): Array[Byte] =
      Serdes.Long.serializer.serialize(topic, data)
  }

  implicit object StringKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[String](SchemaFor[String].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: String): Array[Byte] =
      Serdes.String.serializer.serialize(topic, data)
  }

  implicit object DoubleKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Double](SchemaFor[Double].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: Double): Array[Byte] =
      Serdes.Double.serializer.serialize(topic, data)
  }

  implicit object FloatKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Float](SchemaFor[Float].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: Float): Array[Byte] =
      Serdes.Float.serializer.serialize(topic, data)
  }

  implicit object ByteArrayKafkaPrimitiveSerializer
      extends KafkaPrimitiveSerializer[Array[Byte]](
        SchemaFor[Array[Byte]].schema(DefaultFieldMapper)) {

    override def serialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.serializer.serialize(topic, data)
  }
}

sealed abstract private[codec] class KafkaPrimitiveDeserializer[A]
    extends Deserializer[A] with Serializable {
  override def deserialize(topic: String, data: Array[Byte]): A
}

object KafkaPrimitiveDeserializer {

  implicit object IntKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Int] {

    override def deserialize(topic: String, data: Array[Byte]): Int =
      Serdes.Integer.deserializer.deserialize(topic, data)
  }

  implicit object LongKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Long] {

    override def deserialize(topic: String, data: Array[Byte]): Long =
      Serdes.Long.deserializer.deserialize(topic, data)
  }

  implicit object StringKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[String] {

    override def deserialize(topic: String, data: Array[Byte]): String =
      Serdes.String.deserializer.deserialize(topic, data)
  }

  implicit object DoubleKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Double] {

    override def deserialize(topic: String, data: Array[Byte]): Double =
      Serdes.Double.deserializer.deserialize(topic, data)
  }

  implicit object FloatKafkaPrimitiveDeserializer extends KafkaPrimitiveDeserializer[Float] {

    override def deserialize(topic: String, data: Array[Byte]): Float =
      Serdes.Float.deserializer.deserialize(topic, data)
  }

  implicit object ByteArrayKafkaPrimitiveDeserializer
      extends KafkaPrimitiveDeserializer[Array[Byte]] {

    override def deserialize(topic: String, data: Array[Byte]): Array[Byte] =
      Serdes.ByteArray.deserializer.deserialize(topic, data)
  }
}
