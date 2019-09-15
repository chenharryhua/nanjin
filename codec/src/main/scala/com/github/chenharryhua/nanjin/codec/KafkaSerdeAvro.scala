package com.github.chenharryhua.nanjin.codec

import com.github.chenharryhua.nanjin.codec.CodecException._
import com.sksamuel.avro4s._
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Failure, Success, Try}

final class KafkaSerdeAvro[A: Encoder: Decoder: SchemaFor] extends Serde[A] {
  private[this] val format: RecordFormat[A]        = RecordFormat[A]
  private[this] val schema: Schema                 = AvroSchema[A]
  private[this] val ser: GenericAvroSerializer     = new GenericAvroSerializer
  private[this] val deSer: GenericAvroDeserializer = new GenericAvroDeserializer

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def decode(topic: String, data: Array[Byte]): Try[A] =
    Option(data) match {
      case Some(d) =>
        Try(deSer.deserialize(topic, d)) match {
          case Success(gr) =>
            Try(format.from(gr)) match {
              case a @ Success(_) => a
              case Failure(ex)    => Failure(InvalidObjectException(topic, ex, gr, schema))
            }
          case Failure(ex) => Failure(CorruptedRecordException(topic, ex, schema))
        }
      case None => Success(null.asInstanceOf[A])
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def encode(topic: String, data: A): Try[Array[Byte]] =
    Option(data) match {
      case Some(d) =>
        Try(ser.serialize(topic, format.to(d))) match {
          case ab @ Success(_) => ab
          case Failure(ex)     => Failure(EncodeException(topic, ex, s"${data.toString}", schema))
        }
      case None => Success(null.asInstanceOf[Array[Byte]])
    }

  override def close(): Unit = {
    ser.close()
    deSer.close()
  }
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    ser.configure(configs, isKey)
    deSer.configure(configs, isKey)
  }

  override val serializer: Serializer[A] =
    new Serializer[A] {
      override def close(): Unit = ser.close()

      override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      @throws[CodecException]
      override def serialize(topic: String, data: A): Array[Byte] =
        encode(topic, data).fold(throw _, identity)
    }

  override val deserializer: Deserializer[A] =
    new Deserializer[A] {
      override def close(): Unit = deSer.close()

      override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      @throws[CodecException]
      override def deserialize(topic: String, data: Array[Byte]): A =
        decode(topic, data).fold(throw _, identity)
    }
}
