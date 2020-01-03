package com.github.chenharryhua.nanjin.kafka.codec

import com.sksamuel.avro4s.{DefaultFieldMapper, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
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
}
