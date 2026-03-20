package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, SchemaRegistrySettings}
import com.github.chenharryhua.nanjin.kafka.schema.immigrate
import fs2.kafka.ProducerRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.{Failure, Success}
import io.confluent.kafka.serializers.KafkaAvroSerializer

final private class PushGenericRecord(
  srs: SchemaRegistrySettings,
  topicName: TopicName,
  pair: AvroSchemaPair) {
  val schema: Schema = pair.consumerSchema

  private val topic: String = topicName.value

  private val key_serialize: AnyRef => Array[Byte] =
    pair.key.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val ser = new KafkaAvroSerializer()
        ser.configure(srs.config.asJava, true)
        // java world
        (_: AnyRef) match {
          case gr: GenericRecord =>
            immigrate(pair.key.rawSchema(), gr) match {
              case Success(value) => ser.serialize(topic, value)
              case Failure(ex)    => throw new Exception("unable immigrate key", ex) // scalafix:ok
            }
          case null  => null
          case other =>
            throw new Exception(s"${other.getClass.getName} (key) is not Generic Record") // scalafix:ok
        }

      case Schema.Type.STRING =>
        val ser = Serdes.String().serializer()
        (_: AnyRef) match {
          case null         => null
          case data: String => ser.serialize(topic, data)
          case _            => sys.error("not a string")
        }
      case Schema.Type.INT =>
        val ser = Serdes.Integer().serializer()
        (_: AnyRef) match {
          case null                    => null
          case data: java.lang.Integer => ser.serialize(topic, data)
          case _                       => sys.error("not an integer")
        }
      case Schema.Type.LONG =>
        val ser = Serdes.Long().serializer()
        (_: AnyRef) match {
          case null                 => null
          case data: java.lang.Long => ser.serialize(topic, data)
          case _                    => sys.error("not a long")
        }
      case Schema.Type.FLOAT =>
        val ser = Serdes.Float().serializer()
        (_: AnyRef) match {
          case null                  => null
          case data: java.lang.Float => ser.serialize(topic, data)
          case _                     => sys.error("not a float")
        }
      case Schema.Type.DOUBLE =>
        val ser = Serdes.Double().serializer()
        (_: AnyRef) match {
          case null                   => null
          case data: java.lang.Double => ser.serialize(topic, data)
          case _                      => sys.error("not a double")
        }
      case Schema.Type.BYTES =>
        val ser = Serdes.ByteArray().serializer()
        (_: AnyRef) match {
          case null              => null
          case data: Array[Byte] => ser.serialize(topic, data)
          case _                 => sys.error("not an array[byte]")
        }

      case us => sys.error(s"unsupported key schema: ${us.toString}")
    }

  private val val_serialize: AnyRef => Array[Byte] =
    pair.value.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val ser = new KafkaAvroSerializer()
        ser.configure(srs.config.asJava, false)
        // java world
        (_: AnyRef) match {
          case gr: GenericRecord =>
            immigrate(pair.value.rawSchema(), gr) match {
              case Success(value) => ser.serialize(topic, value)
              case Failure(ex)    => throw new Exception("unable immigrate value", ex) // scalafix:ok
            }
          case null  => null
          case other =>
            throw new Exception(s"${other.getClass.getName} (value) is not Generic Record") // scalafix:ok
        }
      case Schema.Type.STRING =>
        val ser = Serdes.String().serializer()
        (_: AnyRef) match {
          case null         => null
          case data: String => ser.serialize(topic, data)
          case _            => sys.error("not a string")

        }
      case Schema.Type.INT =>
        val ser = Serdes.Integer().serializer()
        (_: AnyRef) match {
          case null                    => null
          case data: java.lang.Integer => ser.serialize(topic, data)
          case _                       => sys.error("not an integer")

        }
      case Schema.Type.LONG =>
        val ser = Serdes.Long().serializer()
        (_: AnyRef) match {
          case null                 => null
          case data: java.lang.Long => ser.serialize(topic, data)
          case _                    => sys.error("not a long")

        }
      case Schema.Type.FLOAT =>
        val ser = Serdes.Float().serializer()
        (_: AnyRef) match {
          case null                  => null
          case data: java.lang.Float => ser.serialize(topic, data)
          case _                     => sys.error("not a float")

        }
      case Schema.Type.DOUBLE =>
        val ser = Serdes.Double().serializer()
        (_: AnyRef) match {
          case null                   => null
          case data: java.lang.Double => ser.serialize(topic, data)
          case _                      => sys.error("not a double")

        }
      case Schema.Type.BYTES =>
        val ser = Serdes.ByteArray().serializer()
        (_: AnyRef) match {
          case null              => null
          case data: Array[Byte] => ser.serialize(topic, data)
          case _                 => sys.error("not a double")

        }

      case us => sys.error(s"unsupported value schema: ${us.toString}")
    }

  /** @param gr
    *   a GenericRecord of NJConsumerRecord
    * @return
    */
  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(topic, key_serialize(gr.get("key")), val_serialize(gr.get("value")))
}
