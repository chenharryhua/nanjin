package com.github.chenharryhua.nanjin.messages.kafka.codec

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import java.util.UUID

private object serializable {
  object stringSerde extends Serde[String] with Serializable {
    override def serializer: Serializer[String] = Serdes.stringSerde.serializer()
    override def deserializer: Deserializer[String] = Serdes.stringSerde.deserializer()
  }

  object uuidSerde extends Serde[UUID] with Serializable {
    override def serializer: Serializer[UUID] = Serdes.uuidSerde.serializer()
    override def deserializer: Deserializer[UUID] = Serdes.uuidSerde.deserializer()
  }

  object intSerde extends Serde[Int] with Serializable {
    override def serializer: Serializer[Int] = Serdes.intSerde.serializer()
    override def deserializer: Deserializer[Int] = Serdes.intSerde.deserializer()
  }

  object longSerde extends Serde[Long] with Serializable {
    override def serializer: Serializer[Long] = Serdes.longSerde.serializer()
    override def deserializer: Deserializer[Long] = Serdes.longSerde.deserializer()
  }

  object doubleSerde extends Serde[Double] with Serializable {
    override def serializer: Serializer[Double] = Serdes.doubleSerde.serializer()
    override def deserializer: Deserializer[Double] = Serdes.doubleSerde.deserializer()
  }

  object floatSerde extends Serde[Float] with Serializable {
    override def serializer: Serializer[Float] = Serdes.floatSerde.serializer()
    override def deserializer: Deserializer[Float] = Serdes.floatSerde.deserializer()
  }

  object shortSerde extends Serde[Short] with Serializable {
    override def serializer: Serializer[Short] = Serdes.shortSerde.serializer()
    override def deserializer: Deserializer[Short] = Serdes.shortSerde.deserializer()
  }

  object byteBufferSerde extends Serde[ByteBuffer] with Serializable {
    override def serializer: Serializer[ByteBuffer] = Serdes.byteBufferSerde.serializer()
    override def deserializer: Deserializer[ByteBuffer] = Serdes.byteBufferSerde.deserializer()
  }

  object byteArraySerde extends Serde[Array[Byte]] with Serializable {
    override def serializer: Serializer[Array[Byte]] = Serdes.byteArraySerde.serializer()
    override def deserializer: Deserializer[Array[Byte]] = Serdes.byteArraySerde.deserializer()
  }
}
