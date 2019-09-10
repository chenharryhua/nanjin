package com.github.chenharryhua.nanjin.codec

import java.{util => ju}

import cats.Invariant
import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, SchemaFor}
import monocle.Prism
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes

import scala.annotation.{implicitAmbiguous, implicitNotFound}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

sealed trait KafkaCodec[A] {
  def encode(a: A): Array[Byte]
  def decode(ab: Array[Byte]): A
  final def tryDecode(ab: Array[Byte]): Try[A] =
    Option(ab)
      .fold[Try[Array[Byte]]](Failure(CodecException.DecodingNullException))(Success(_))
      .flatMap(x => Try(decode(x)))
  final val prism: Prism[Array[Byte], A] =
    Prism[Array[Byte], A](x => Try(decode(x)).toOption)(encode)
}

object KafkaCodec {
  implicit val invariantCodec: Invariant[KafkaCodec] = new Invariant[KafkaCodec] {
    override def imap[A, B](fa: KafkaCodec[A])(f: A => B)(g: B => A): KafkaCodec[B] =
      new KafkaCodec[B] {
        override def encode(b: B): Array[Byte]  = fa.encode(g(b))
        override def decode(ab: Array[Byte]): B = f(fa.decode(ab))
      }
  }
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
  "Could not find an instance of SerdeOf[${A}], primitive types and case class are supprted")
sealed abstract class SerdeOf[A](schema: Schema) extends Serializable {
  def serializer: Serializer[A]

  def deserializer: Deserializer[A]

  final def asKey(props: Map[String, String]): KeySerde[A] =
    KeySerde(schema, serializer, deserializer, props)

  final def asValue(props: Map[String, String]): ValueSerde[A] =
    ValueSerde(schema, serializer, deserializer, props)
}

sealed private[codec] trait SerdeOfPriority0 {

  import com.sksamuel.avro4s.{Decoder, Encoder}

  implicit def kavroSerde[A: Encoder: Decoder: SchemaFor]: SerdeOf[A] = {
    val serde: Serde[A] = new KafkaSerdeAvro[A]
    new SerdeOf[A](AvroSchema[A]) {
      override val deserializer: Deserializer[A] = serde.deserializer
      override val serializer: Serializer[A]     = serde.serializer
    }
  }

  @implicitAmbiguous("KJson should import io.circe.generic.auto._")
  implicit def kjsonSerdeWasNotInferred[A](implicit ev: A <:< KJson[_]): SerdeOf[A] =
    sys.error("compilation time only")
}

sealed private[codec] trait SerdeOfPriority1 extends SerdeOfPriority0 {

  import io.circe.{Decoder, Encoder}

  implicit def kjsonSerde[A: Decoder: Encoder]: SerdeOf[KJson[A]] = {
    val serde: Serde[KJson[A]] = new KafkaSerdeJson[A]
    new SerdeOf[KJson[A]](SchemaFor[String].schema(DefaultFieldMapper)) {
      override val deserializer: Deserializer[KJson[A]] = serde.deserializer()
      override val serializer: Serializer[KJson[A]]     = serde.serializer()
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
