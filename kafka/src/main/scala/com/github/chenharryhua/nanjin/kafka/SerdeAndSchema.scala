package com.github.chenharryhua.nanjin.kafka

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import cats.Show
sealed trait SerdeAndSchema[A] {
  def serde: Serde[A]
  def schema: Schema
}

trait Priority0 {
  import com.sksamuel.avro4s.{Decoder, Encoder}

  implicit def kavroSerde[A: SchemaFor: Encoder: Decoder]: SerdeAndSchema[KAvro[A]] =
    new SerdeAndSchema[KAvro[A]] {
      override val schema: Schema         = AvroSchema[A]
      override val serde: Serde[KAvro[A]] = new KafkaAvroSerde[A](schema)
    }
}

trait Priority1 extends Priority0 {
  import io.circe.{Decoder, Encoder}
  implicit def kjsonSerde[A: Decoder: Encoder]: SerdeAndSchema[KJson[A]] =
    new SerdeAndSchema[KJson[A]] {
      override def schema: Schema         = SchemaFor[String].schema
      override def serde: Serde[KJson[A]] = new KafkaJsonSerde[A]
    }
}

object SerdeAndSchema extends Priority1 {
  implicit object stringSerde extends SerdeAndSchema[String] {
    override val serde: Serde[String] = Serdes.String
    override val schema: Schema       = SchemaFor[String].schema
  }

  implicit object intSerde extends SerdeAndSchema[Int] {
    override val serde: Serde[Int] = Serdes.Integer
    override val schema: Schema    = SchemaFor[Int].schema
  }

  implicit object longSerde extends SerdeAndSchema[Long] {
    override val serde: Serde[Long] = Serdes.Long
    override val schema: Schema     = SchemaFor[Long].schema
  }

  implicit object doubleSerde extends SerdeAndSchema[Double] {
    override val serde: Serde[Double] = Serdes.Double
    override val schema: Schema       = SchemaFor[Double].schema
  }

  implicit object byteArraySerde extends SerdeAndSchema[Array[Byte]] {
    override val serde: Serde[Array[Byte]] = Serdes.ByteArray
    override val schema: Schema            = SchemaFor[Array[Byte]].schema
  }

  implicit def showSerdeAndSchema[A]: Show[SerdeAndSchema[A]] =
    (t: SerdeAndSchema[A]) => s"""
                                 |serde and schema
                                 |serde:  ${t.serde}
                                 |schema: ${t.schema.toString()}""".stripMargin
}
