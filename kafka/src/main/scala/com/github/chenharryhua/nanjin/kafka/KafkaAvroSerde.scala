package com.github.chenharryhua.nanjin.kafka
import cats.Show
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.CodecException._
import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Failure, Success, Try}
import org.apache.avro.Schema
import shapeless.HasProductGeneric

final case class KAvro[A: Encoder: Decoder: SchemaFor](value: A)

object KAvro {
  implicit def showKafkaAvro[A: Show]: Show[KAvro[A]] = (t: KAvro[A]) => s"KAvro(${t.value.show})"
}

final class KafkaAvroSerde[A: Encoder: Decoder: SchemaFor](csrClient: CachedSchemaRegistryClient)
    extends Serde[KAvro[A]] {
  private[this] val format: RecordFormat[A]      = RecordFormat[A]
  private[this] val schema: Schema               = AvroSchema[A]
  private[this] val ser: KafkaAvroSerializer     = new KafkaAvroSerializer(csrClient)
  private[this] val deSer: KafkaAvroDeserializer = new KafkaAvroDeserializer(csrClient)

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def decode(topic: String, data: Array[Byte]): Try[KAvro[A]] =
    Option(data) match {
      case None => Success(null.asInstanceOf[KAvro[A]])
      case Some(d) =>
        Try(deSer.deserialize(topic, d)) match {
          case Success(gr: GenericRecord) =>
            Try(format.from(gr)) match {
              case Success(v) => Success(KAvro(v))
              case Failure(ex) =>
                Failure(InvalidObjectException(s"""|decode avro failed
                                                   |topic:         $topic
                                                   |error:         ${ex.getMessage} 
                                                   |GenericRecord: ${gr.toString}
                                                   |schema:        ${schema.toString}""".stripMargin))
            }
          case Success(gr) =>
            Failure(InvalidGenericRecordException(s"""|decode avro failed
                                                      |topic:         $topic
                                                      |error:         schema mismatch
                                                      |GenericRecord: ${gr.toString}
                                                      |schema:        ${schema.toString}""".stripMargin))
          case Failure(ex) =>
            Failure(CorruptedRecordException(s"""|decode avro failed
                                                 |topic: $topic 
                                                 |error: ${ex.getMessage}
                                                 |schema: ${schema.toString}""".stripMargin))
        }
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  private[this] def encode(topic: String, data: KAvro[A]): Try[Array[Byte]] =
    Option(data).flatMap(x => Option(x.value)) match {
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

  override val serializer: Serializer[KAvro[A]] =
    new Serializer[KAvro[A]] {
      override def close(): Unit = ser.close()

      override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      @throws[CodecException]
      override def serialize(topic: String, data: KAvro[A]): Array[Byte] =
        encode(topic, data).fold(throw _, identity)
    }

  override val deserializer: Deserializer[KAvro[A]] =
    new Deserializer[KAvro[A]] {
      override def close(): Unit =
        deSer.close()

      override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      @throws[CodecException]
      override def deserialize(topic: String, data: Array[Byte]): KAvro[A] =
        decode(topic, data).fold(throw _, identity)
    }
}
