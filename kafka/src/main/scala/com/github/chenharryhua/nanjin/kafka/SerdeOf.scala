package com.github.chenharryhua.nanjin.kafka

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

sealed abstract class SerdeOf[A](val schema: Schema) extends Serde[A] with Serializable {
  val serializer: Serializer[A]
  val deserializer: Deserializer[A]
}

trait Priority0 {
  import com.sksamuel.avro4s.{Decoder, Encoder}

  implicit def kavroSerde[A: SchemaFor: Encoder: Decoder]: SerdeOf[KAvro[A]] = {
    val schema: Schema         = AvroSchema[A]
    val serde: Serde[KAvro[A]] = new KafkaAvroSerde[A](schema)
    new SerdeOf[KAvro[A]](schema) {
      override val deserializer: Deserializer[KAvro[A]] = serde.deserializer
      override val serializer: Serializer[KAvro[A]]     = serde.serializer
    }
  }
}

trait Priority1 extends Priority0 {
  import io.circe.{Decoder, Encoder}
  implicit def kjsonSerde[A: Decoder: Encoder]: SerdeOf[KJson[A]] = {
    val schema: Schema         = SchemaFor[String].schema
    val serde: Serde[KJson[A]] = new KafkaJsonSerde[A]
    new SerdeOf[KJson[A]](schema) {
      override val deserializer: Deserializer[KJson[A]] = serde.deserializer()
      override val serializer: Serializer[KJson[A]]     = serde.serializer()
    }
  }
}

object SerdeOf extends Priority1 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  implicit object kstringSerde extends SerdeOf[String](SchemaFor[String].schema) {
    override val deserializer: Deserializer[String] = Serdes.String.deserializer()
    override val serializer: Serializer[String]     = Serdes.String.serializer()
  }

  implicit object kintSerde extends SerdeOf[Int](SchemaFor[Int].schema) {
    override val deserializer: Deserializer[Int] = Serdes.Integer.deserializer()
    override val serializer: Serializer[Int]     = Serdes.Integer.serializer()
  }

  implicit object klongSerde extends SerdeOf[Long](SchemaFor[Long].schema) {
    override val deserializer: Deserializer[Long] = Serdes.Long.deserializer()
    override val serializer: Serializer[Long]     = Serdes.Long.serializer()
  }

  implicit object kdoubleSerde extends SerdeOf[Double](SchemaFor[Double].schema) {
    override val deserializer: Deserializer[Double] = Serdes.Double.deserializer()
    override val serializer: Serializer[Double]     = Serdes.Double.serializer()
  }

  implicit object kbyteArraySerde extends SerdeOf[Array[Byte]](SchemaFor[Array[Byte]].schema) {
    override val deserializer: Deserializer[Array[Byte]] = Serdes.ByteArray.deserializer()
    override val serializer: Serializer[Array[Byte]]     = Serdes.ByteArray.serializer()
  }
}
