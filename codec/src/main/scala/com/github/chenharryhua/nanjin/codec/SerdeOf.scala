package com.github.chenharryhua.nanjin.codec

import java.{util => ju}

import cats.tagless.autoInvariant
import com.sksamuel.avro4s.{
  AvroSchema,
  DefaultFieldMapper,
  SchemaFor,
  Encoder => AvroEncoder,
  Decoder => AvroDecoder
}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.Prism
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

import scala.annotation.{compileTimeOnly, implicitAmbiguous, implicitNotFound}
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

@autoInvariant
sealed trait KafkaCodec[A] {
  def encode(a: A): Array[Byte]
  def decode(ab: Array[Byte]): A

  final def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(CodecException.DecodingNullException))(x => Try(decode(x)))

  final val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

sealed abstract class KafkaSerde[A](serializer: Serializer[A], deserializer: Deserializer[A])
    extends Serde[A] {

  final override def configure(configs: ju.Map[String, _], isKey: Boolean): Unit = {
    serializer.configure(configs, isKey)
    deserializer.configure(configs, isKey)
  }

  final override def close(): Unit = {
    serializer.close()
    deserializer.close()
  }

  final def codec(topicName: String): KafkaCodec[A] = new KafkaCodec[A] {
    override def encode(a: A): Array[Byte]  = serializer.serialize(topicName, a)
    override def decode(ab: Array[Byte]): A = deserializer.deserialize(topicName, ab)
  }
}

final case class KeySerde[A](
  schema: Schema,
  serializer: Serializer[A],
  deserializer: Deserializer[A],
  props: Map[String, String])
    extends KafkaSerde[A](serializer, deserializer) {
  configure(props.asJava, isKey = true)
}

final case class ValueSerde[A](
  schema: Schema,
  serializer: Serializer[A],
  deserializer: Deserializer[A],
  props: Map[String, String])
    extends KafkaSerde[A](serializer, deserializer) {
  configure(props.asJava, isKey = false)
}

@implicitNotFound(
  "Could not find an instance of SerdeOf[${A}], primitive types and case classes are supported")
sealed abstract class SerdeOf[A](schema: Schema) extends Serializable {
  def serializer: Serializer[A]

  def deserializer: Deserializer[A]

  final def asKey(props: Map[String, String]): KeySerde[A] =
    KeySerde(schema, serializer, deserializer, props)

  final def asValue(props: Map[String, String]): ValueSerde[A] =
    ValueSerde(schema, serializer, deserializer, props)
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

  implicit def kmanualavroSerde[A: ManualAvroSchema]: SerdeOf[A] = {
    val inst: ManualAvroSchema[A] = ManualAvroSchema[A]
    import inst.{decoder, encoder}
    val serde: Serde[A] = new KafkaSerdeAvro[A](inst.schema)
    new SerdeOf[A](inst.schema) {
      override val deserializer: Deserializer[A] = serde.deserializer()
      override val serializer: Serializer[A]     = serde.serializer()
    }
  }
}

object SerdeOf extends SerdeOfPriority1 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

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
