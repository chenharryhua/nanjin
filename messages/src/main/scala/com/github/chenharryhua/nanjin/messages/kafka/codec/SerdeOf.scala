package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.github.chenharryhua.nanjin.messages.kafka.KeyValueTag
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import monocle.Prism
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

final class NJCodec[A](val topicName: String, val serde: NJSerde[A]) extends Serializable {
  def encode(a: A): Array[Byte]  = serde.serializer.serialize(topicName, a)
  def decode(ab: Array[Byte]): A = serde.deserializer.deserialize(topicName, ab)

  def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(new NullPointerException))(x => Try(decode(x)))

  val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

sealed abstract class NJSerde[A](
  val tag: KeyValueTag,
  val schemaFor: SchemaFor[A],
  val configProps: Map[String, String],
  override val serializer: Serializer[A],
  override val deserializer: Deserializer[A])
    extends Serde[A] with Serializable {

  serializer.configure(configProps.asJava, tag.isKey)
  deserializer.configure(configProps.asJava, tag.isKey)

  final def codec(topicName: String) = new NJCodec[A](topicName, this)
}

@implicitNotFound(
  "Could not find an instance of SerdeOf[${A}], primitive types and case classes are supported")
sealed abstract class SerdeOf[A](val schemaFor: SchemaFor[A]) extends Serializable {
  val serializer: Serializer[A]
  val deserializer: Deserializer[A]

  val avroEncoder: AvroEncoder[A]
  val avroDecoder: AvroDecoder[A]

  final def asKey(props: Map[String, String]): NJSerde[A] =
    new NJSerde(KeyValueTag.Key, schemaFor, props, serializer, deserializer) {}

  final def asValue(props: Map[String, String]): NJSerde[A] =
    new NJSerde(KeyValueTag.Value, schemaFor, props, serializer, deserializer) {}
}

sealed private[codec] trait SerdeOfPriority0 {

  implicit final def inferedAvroSerde[A: AvroEncoder: AvroDecoder]: SerdeOf[A] = {
    val ser: KafkaSerializer[A]     = KafkaSerializer[A](AvroEncoder[A])
    val deSer: KafkaDeserializer[A] = KafkaDeserializer[A](AvroDecoder[A])
    new SerdeOf[A](ser.avroEncoder.schemaFor) {
      override val avroDecoder: AvroDecoder[A]   = deSer.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = ser.avroEncoder
      override val deserializer: Deserializer[A] = deSer
      override val serializer: Serializer[A]     = ser
    }
  }
}

object SerdeOf extends SerdeOfPriority0 {
  def apply[A](implicit ev: SerdeOf[A]): SerdeOf[A] = ev

  def apply[A](inst: WithAvroSchema[A]): SerdeOf[A] = {
    val ser: KafkaSerializer[A]     = KafkaSerializer[A](inst.avroEncoder, inst.schemaFor)
    val deSer: KafkaDeserializer[A] = KafkaDeserializer[A](inst.avroDecoder, inst.schemaFor)
    new SerdeOf[A](inst.avroDecoder.schemaFor) {
      override val avroDecoder: AvroDecoder[A]   = deSer.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = ser.avroEncoder
      override val deserializer: Deserializer[A] = deSer
      override val serializer: Serializer[A]     = ser
    }
  }

  implicit def knownSerde[A: KafkaSerializer: KafkaDeserializer]: SerdeOf[A] = {
    val ser   = KafkaSerializer[A]
    val deser = KafkaDeserializer[A]
    new SerdeOf[A](ser.avroEncoder.schemaFor) {
      override val avroDecoder: AvroDecoder[A]   = deser.avroDecoder
      override val avroEncoder: AvroEncoder[A]   = ser.avroEncoder
      override val deserializer: Deserializer[A] = deser
      override val serializer: Serializer[A]     = ser
    }
  }
}
