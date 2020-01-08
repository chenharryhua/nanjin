package com.github.chenharryhua.nanjin.kafka.codec

import java.{util => ju}

import com.github.chenharryhua.nanjin.kafka.{KJson, ManualAvroSchema,TopicName}
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

sealed private[codec] class KafkaCodec[A](topicName: TopicName, serde: KafkaSerde[A]) {
  final def encode(a: A): Array[Byte]  = serde.serializer.serialize(topicName.value, a)
  final def decode(ab: Array[Byte]): A = serde.deserializer.deserialize(topicName.value, ab)

  final def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab).fold[Try[A]](Failure(CodecException.DecodingNullException))(x => Try(decode(x)))

  final val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)

  final def show: String = s"KafkaCodec(topicName = $topicName, serde = ${serde.show})"
}

object KafkaCodec {

  final class Key[A](val topicName: TopicName, val serde: KafkaSerde.Key[A])
      extends KafkaCodec[A](topicName, serde) with Serializable

  final class Value[A](val topicName: TopicName, val serde: KafkaSerde.Value[A])
      extends KafkaCodec[A](topicName, serde) with Serializable

}

sealed private[codec] trait KafkaSerde[A] extends Serde[A] {
  override def serializer: Serializer[A]
  override def deserializer: Deserializer[A]

  final override def configure(configs: ju.Map[String, _], isKey: Boolean): Unit = {
    serializer.configure(configs, isKey)
    deserializer.configure(configs, isKey)
  }

  final override def close(): Unit = {
    serializer.close()
    deserializer.close()
  }

  def show: String
}

object KafkaSerde {

  final class Key[A](
    val schema: Schema,
    val configProps: Map[String, String],
    override val serializer: Serializer[A],
    override val deserializer: Deserializer[A])
      extends KafkaSerde[A] with Serializable {

    configure(configProps.asJava, isKey = true)

    def codec(topicName: TopicName): KafkaCodec.Key[A] =
      new KafkaCodec.Key[A](topicName, this)

    override def show: String = s"KafkaSerde.Key(schema = $schema, configProps = $configProps)"
  }

  final class Value[A](
    val schema: Schema,
    val configProps: Map[String, String],
    override val serializer: Serializer[A],
    override val deserializer: Deserializer[A])
      extends KafkaSerde[A] with Serializable {

    configure(configProps.asJava, isKey = false)

    def codec(topicName: TopicName): KafkaCodec.Value[A] =
      new KafkaCodec.Value[A](topicName, this)

    override def show: String = s"KafkaSerde.Value(schema = $schema, configProps = $configProps)"
  }
}

@implicitNotFound(
  "Could not find an instance of SerdeOf[${A}], primitive types and case classes are supported")
sealed abstract class SerdeOf[A](val schema: Schema) extends Serializable {
  def serializer: Serializer[A]

  def deserializer: Deserializer[A]

  final def asKey(props: Map[String, String]): KafkaSerde.Key[A] =
    new KafkaSerde.Key(schema, props, serializer, deserializer)

  final def asValue(props: Map[String, String]): KafkaSerde.Value[A] =
    new KafkaSerde.Value(schema, props, serializer, deserializer)
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

  implicit def primitiveSerde[A: KafkaPrimitiveSerializer: KafkaPrimitiveDeserializer]: SerdeOf[A] =
    new SerdeOf[A](implicitly[KafkaPrimitiveSerializer[A]].schema) {
      override def deserializer: Deserializer[A] = implicitly[KafkaPrimitiveDeserializer[A]]
      override def serializer: Serializer[A]     = implicitly[KafkaPrimitiveSerializer[A]]
    }
}
