package com.github.chenharryhua.nanjin.kafka
import com.github.chenharryhua.nanjin.kafka.CodecException._
import com.sksamuel.avro4s._
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Failure, Success, Try}

final class KafkaAvroSerde[A: Encoder: Decoder: SchemaFor](srSettings: SchemaRegistrySettings)
    extends Serde[A] {
  private[this] val format: RecordFormat[A] = RecordFormat[A]
  private[this] val schema: Schema          = AvroSchema[A]
  private[this] val ser: KafkaAvroSerializer =
    new KafkaAvroSerializer(srSettings.csrClient.value)
  private[this] val deSer: KafkaAvroDeserializer =
    new KafkaAvroDeserializer(srSettings.csrClient.value)

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def decode(topic: String, data: Array[Byte]): Try[A] =
    Option(data) match {
      case None => Success(null.asInstanceOf[A])
      case Some(d) =>
        Try(deSer.deserialize(topic, d)) match {
          case Success(gr: GenericRecord) =>
            Try(format.from(gr)) match {
              case Success(v) => Success(v)
              case Failure(ex) =>
                Failure(InvalidObjectException(s"""|decode avro failed:
                                                   |topic:         $topic
                                                   |error:         ${ex.getMessage} 
                                                   |GenericRecord: ${gr.toString}
                                                   |schema:        ${schema.toString}""".stripMargin))
            }
          case Success(gr) =>
            Failure(InvalidGenericRecordException(s"""|decode avro failed:
                                                      |topic:         $topic
                                                      |error:         invalid generic record
                                                      |GenericRecord: ${gr.toString}
                                                      |schema:        ${schema.toString}""".stripMargin))
          case Failure(ex) =>
            Failure(CorruptedRecordException(s"""|decode avro failed:
                                                 |topic:  $topic 
                                                 |error:  ${ex.getMessage}
                                                 |schema: ${schema.toString}""".stripMargin))
        }
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def encode(topic: String, data: A): Try[Array[Byte]] =
    Option(data) match {
      case None => Success(null.asInstanceOf[Array[Byte]])
      case Some(d) =>
        Try(ser.serialize(topic, format.to(d))) match {
          case v @ Success(_) => v
          case Failure(ex) =>
            Failure(EncodeException(s"""|encode avro failed: 
                                        |topic:  $topic
                                        |error:  ${ex.getMessage}
                                        |data:   $data
                                        |schema: ${schema.toString()}""".stripMargin))
        }
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
      override def close(): Unit =
        deSer.close()

      override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      @throws[CodecException]
      override def deserialize(topic: String, data: Array[Byte]): A =
        decode(topic, data).fold(throw _, identity)
    }
}
