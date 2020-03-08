package com.github.chenharryhua.nanjin.kafka.codec

import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.common.KeyValueTag
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

import scala.annotation.{compileTimeOnly, implicitAmbiguous, implicitNotFound}
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

final class NJCodec[A](val topicName: TopicName, val serde: NJSerde[A]) extends Serializable {
  def encode(a: A): Array[Byte]  = serde.serializer.serialize(topicName.value, a)
  def decode(ab: Array[Byte]): A = serde.deserializer.deserialize(topicName.value, ab)

  def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(CodecException.DecodingNullException))(x => Try(decode(x)))

  val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

sealed abstract class NJSerde[A](
  val tag: KeyValueTag,
  val schema: Schema,
  val configProps: Map[String, String],
  override val serializer: Serializer[A],
  override val deserializer: Deserializer[A])
    extends Serde[A] with Serializable {

  serializer.configure(configProps.asJava, tag.isKey)
  deserializer.configure(configProps.asJava, tag.isKey)

  final def codec(topicName: TopicName) = new NJCodec[A](topicName, this)
}

@implicitNotFound(
  "Could not find an instance of SerdeOf[${A}], primitive types and case classes are supported")
sealed abstract class SerdeOf[A](val schema: Schema) extends Serializable {
  val serializer: Serializer[A]
  val deserializer: Deserializer[A]

  val avroEncoder: AvroEncoder[A]
  val avroDecoder: AvroDecoder[A]

  final def asKey(props: Map[String, String]): NJSerde[A] =
    new NJSerde(KeyValueTag.Key, schema, props, serializer, deserializer) {}

  final def asValue(props: Map[String, String]): NJSerde[A] =
    new NJSerde(KeyValueTag.Value, schema, props, serializer, deserializer) {}
}

sealed private[codec] trait SerdeOfPriority0 {

  implicit final def kavroSerde[A: AvroDecoder: AvroEncoder: SchemaFor]: SerdeOf[A] = {
    val serde: KafkaSerdeAvro[A] = new KafkaSerdeAvro[A](AvroSchema[A])
    new SerdeOf[A](AvroSchema[A]) {
      override val avroDecoder: AvroDecoder[A]   = serde.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = serde.avroEncoder
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

  implicit final def kjsonSerde[A: JsonDecoder: JsonEncoder: AvroDecoder: AvroEncoder]
    : SerdeOf[KJson[A]] = {
    val serde: Serde[KJson[A]] = new KafkaSerdeJson[A]
    new SerdeOf[KJson[A]](SchemaFor[String].schema(DefaultFieldMapper)) {
      override val avroDecoder: AvroDecoder[KJson[A]]   = implicitly[AvroDecoder[KJson[A]]]
      override val avroEncoder: AvroEncoder[KJson[A]]   = implicitly[AvroEncoder[KJson[A]]]
      override val deserializer: Deserializer[KJson[A]] = serde.deserializer()
      override val serializer: Serializer[KJson[A]]     = serde.serializer()
    }
  }
}

object SerdeOf extends SerdeOfPriority1 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  def apply[A](inst: ManualAvroSchema[A]): SerdeOf[A] = {
    import inst.{avroDecoder, avroEncoder}
    val serde: KafkaSerdeAvro[A] = new KafkaSerdeAvro[A](inst.schema)
    new SerdeOf[A](inst.schema) {
      override val avroDecoder: AvroDecoder[A]   = serde.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = serde.avroEncoder
      override val deserializer: Deserializer[A] = serde.deserializer
      override val serializer: Serializer[A]     = serde.serializer
    }
  }

  implicit def primitiveSerde[A: KafkaPrimitiveSerializer: KafkaPrimitiveDeserializer]
    : SerdeOf[A] = {
    val ser   = implicitly[KafkaPrimitiveSerializer[A]]
    val deser = implicitly[KafkaPrimitiveDeserializer[A]]
    new SerdeOf[A](implicitly[KafkaPrimitiveSerializer[A]].schema) {
      override val avroDecoder: AvroDecoder[A]   = deser.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = ser.avroEncoder
      override val deserializer: Deserializer[A] = deser
      override val serializer: Serializer[A]     = ser
    }
  }
}
