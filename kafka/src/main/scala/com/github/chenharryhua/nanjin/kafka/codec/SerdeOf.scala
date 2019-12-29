package com.github.chenharryhua.nanjin.kafka.codec

import java.{util => ju}

import com.github.chenharryhua.nanjin.kafka.ManualAvroSchema
import com.sksamuel.avro4s.{
  AvroSchema,
  DefaultFieldMapper,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.Prism
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

import scala.annotation.{compileTimeOnly, implicitAmbiguous, implicitNotFound}
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

sealed abstract private[codec] class KafkaCodec[A](topicName: String, serde: KafkaSerde[A]) {
  final def encode(a: A): Array[Byte]  = serde.serializer.serialize(topicName, a)
  final def decode(ab: Array[Byte]): A = serde.deserializer.deserialize(topicName, ab)

  final def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(CodecException.DecodingNullException))(x => Try(decode(x)))

  final val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

object KafkaCodec {

  final case class Key[A](topicName: String, serde: KafkaSerde.Key[A])
      extends KafkaCodec[A](topicName, serde)

  final case class Value[A](topicName: String, serde: KafkaSerde.Value[A])
      extends KafkaCodec[A](topicName, serde)

}

sealed abstract private[codec] class KafkaSerde[A](schema: Schema, configProps: Map[String, String])
    extends Serde[A] {

  final override def configure(configs: ju.Map[String, _], isKey: Boolean): Unit = {
    serializer.configure(configs, isKey)
    deserializer.configure(configs, isKey)
  }

  final override def close(): Unit = {
    serializer.close()
    deserializer.close()
  }
}

object KafkaSerde {

  final case class Key[A](
    schema: Schema,
    configProps: Map[String, String],
    override val serializer: Serializer[A],
    override val deserializer: Deserializer[A]
  ) extends KafkaSerde[A](schema, configProps) {
    configure(configProps.asJava, isKey = true)

    def codec(topicName: String): KafkaCodec.Key[A] =
      KafkaCodec.Key[A](topicName, this)

  }

  final case class Value[A](
    schema: Schema,
    configProps: Map[String, String],
    override val serializer: Serializer[A],
    override val deserializer: Deserializer[A])
      extends KafkaSerde[A](schema, configProps) {
    configure(configProps.asJava, isKey = false)

    def codec(topicName: String): KafkaCodec.Value[A] =
      KafkaCodec.Value[A](topicName, this)
  }
}

@implicitNotFound(
  "Could not find an instance of SerdeOf[${A}], primitive types and case classes are supported")
sealed abstract class SerdeOf[A](val schema: Schema) extends Serializable {
  def serializer: Serializer[A]

  def deserializer: Deserializer[A]

  final def asKey(props: Map[String, String]): KafkaSerde.Key[A] =
    KafkaSerde.Key(schema, props, serializer, deserializer)

  final def asValue(props: Map[String, String]): KafkaSerde.Value[A] =
    KafkaSerde.Value(schema, props, serializer, deserializer)
}

sealed private[codec] trait SerdeOfPriority0 {

  implicit def kavroSerde[A: AvroDecoder: AvroEncoder: SchemaFor]: SerdeOf[A] = {
    val serde: Serde[A] = new KafkaSerdeAvro[A](AvroSchema[A])
    new SerdeOf[A](AvroSchema[A]) {
      override val deserializer: Deserializer[A] = serde.deserializer
      override val serializer: Serializer[A]     = serde.serializer
    }
  }

  @implicitAmbiguous("KJson should import io.circe.generic.auto._")
  @compileTimeOnly("should not refer")
  implicit def kjsonSerdeWasNotInferred[A](implicit ev: A <:< KJson[_]): SerdeOf[A] =
    sys.error("compilation time only")
}

sealed private[codec] trait SerdeOfPriority1 extends SerdeOfPriority0 {

  implicit def kjsonSerde[A: JsonDecoder: JsonEncoder]: SerdeOf[KJson[A]] = {
    val serde: Serde[KJson[A]] = new KafkaSerdeJson[A]
    new SerdeOf[KJson[A]](SchemaFor[String].schema(DefaultFieldMapper)) {
      override val deserializer: Deserializer[KJson[A]] = serde.deserializer()
      override val serializer: Serializer[KJson[A]]     = serde.serializer()
    }
  }
}

object SerdeOf extends SerdeOfPriority1 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  def apply[A](inst: ManualAvroSchema[A]): SerdeOf[A] = {
    import inst.{decoder, encoder}
    val serde: Serde[A] = new KafkaSerdeAvro[A](inst.schema)
    new SerdeOf[A](inst.schema) {
      override val deserializer: Deserializer[A] = serde.deserializer()
      override val serializer: Serializer[A]     = serde.serializer()
    }
  }

  implicit object kstringSerde
      extends SerdeOf[String](SchemaFor[String].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[String] = Serdes.String.deserializer()
    override val serializer: Serializer[String]     = Serdes.String.serializer()
  }

  implicit object kintSerde extends SerdeOf[Int](SchemaFor[Int].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[Int] = Serdes.Integer.deserializer()
    override val serializer: Serializer[Int]     = Serdes.Integer.serializer()
  }

  implicit object klongSerde extends SerdeOf[Long](SchemaFor[Long].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[Long] = Serdes.Long.deserializer()
    override val serializer: Serializer[Long]     = Serdes.Long.serializer()
  }

  implicit object kdoubleSerde
      extends SerdeOf[Double](SchemaFor[Double].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[Double] = Serdes.Double.deserializer()
    override val serializer: Serializer[Double]     = Serdes.Double.serializer()
  }

  implicit object kfloatSerde extends SerdeOf[Float](SchemaFor[Float].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[Float] = Serdes.Float.deserializer()
    override val serializer: Serializer[Float]     = Serdes.Float.serializer()
  }

  implicit object kbyteArraySerde
      extends SerdeOf[Array[Byte]](SchemaFor[Array[Byte]].schema(DefaultFieldMapper)) {
    override val deserializer: Deserializer[Array[Byte]] = Serdes.ByteArray.deserializer()
    override val serializer: Serializer[Array[Byte]]     = Serdes.ByteArray.serializer()
  }
}
