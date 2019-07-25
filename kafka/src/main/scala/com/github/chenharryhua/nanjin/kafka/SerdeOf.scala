package com.github.chenharryhua.nanjin.kafka

import cats.Show
import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

sealed abstract class SerdeOf[A](schema: Schema, serde: Serde[A])
    extends Serde[A] with Serializable {
  final val serializer: Serializer[A]     = serde.serializer
  final val deserializer: Deserializer[A] = serde.deserializer
  final val asKey                         = Key(serde, schema)
  final val asValue                       = Value(serde, schema)
}

final case class Key[A](private val serde: Serde[A], schema: Schema)
    extends SerdeOf[A](schema, serde)

object Key {
  implicit def showKey[A]: Show[Key[A]] =
    (t: Key[A]) => s"""
                      |key serde and schema
                      |serde:  ${t.serde}
                      |schema: ${t.schema.toString}
       """.stripMargin
}

final case class Value[A](private val serde: Serde[A], schema: Schema)
    extends SerdeOf[A](schema, serde)

object Value {
  implicit def showValue[A]: Show[Value[A]] =
    (t: Value[A]) => s"""
                        |value serde and schema
                        |serde:  ${t.serde}
                        |schema: ${t.schema.toString}
       """.stripMargin
}

trait Priority0 {
  import com.sksamuel.avro4s.{Decoder, Encoder}

  implicit def kavroSerde[A: SchemaFor: Encoder: Decoder]: SerdeOf[KAvro[A]] = {
    val schema: Schema         = AvroSchema[A]
    val serde: Serde[KAvro[A]] = new KafkaAvroSerde[A](schema)
    new SerdeOf[KAvro[A]](schema, serde) {}
  }
}

trait Priority1 extends Priority0 {
  import io.circe.{Decoder, Encoder}
  implicit def kjsonSerde[A: Decoder: Encoder]: SerdeOf[KJson[A]] = {
    val schema: Schema         = SchemaFor[String].schema
    val serde: Serde[KJson[A]] = new KafkaJsonSerde[A]
    new SerdeOf[KJson[A]](schema, serde) {}
  }
}

object SerdeOf extends Priority1 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev
  implicit object stringSerde extends SerdeOf[String](SchemaFor[String].schema, Serdes.String) {}
  implicit object intSerde extends SerdeOf[Int](SchemaFor[Int].schema, Serdes.Integer) {}
  implicit object longSerde extends SerdeOf[Long](SchemaFor[Long].schema, Serdes.Long) {}
  implicit object doubleSerde extends SerdeOf[Double](SchemaFor[Double].schema, Serdes.Double) {}
  implicit object byteArraySerde
      extends SerdeOf[Array[Byte]](SchemaFor[Array[Byte]].schema, Serdes.ByteArray) {}
}
