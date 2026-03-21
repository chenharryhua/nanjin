package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.schema.immigrate
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, SchemaRegistrySettings, TopicName}
import fs2.kafka.ProducerRecord
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.{Failure, Success}

final private class PushGenericRecord(
  srs: SchemaRegistrySettings,
  topicName: TopicName,
  pair: AvroSchemaPair) {
  val schema: Schema = pair.consumerSchema

  private val topic: String = topicName.value

  private def getEncoder(skm: Schema)(data: AnyRef): Array[Byte] =
    skm.getType match {
      case Schema.Type.RECORD =>
        val ser = new KafkaAvroSerializer()
        ser.configure(srs.config.asJava, true)
        // java world
        data match {
          case null              => null
          case gr: GenericRecord =>
            immigrate(pair.key.rawSchema(), gr) match {
              case Success(value) => ser.serialize(topic, value)
              case Failure(ex)    => throw new Exception("unable immigrate key", ex) // scalafix:ok
            }
          case other =>
            throw new Exception(s"${other.getClass.getName} (key) is not Generic Record") // scalafix:ok
        }
      case Schema.Type.STRING =>
        data match {
          case null         => null
          case data: String => Serdes.String().serializer().serialize(topic, data)
          case _            => sys.error("not a string")
        }
      case Schema.Type.BYTES =>
        data match {
          case null              => null
          case data: Array[Byte] => Serdes.ByteArray().serializer().serialize(topic, data)
          case _                 => sys.error("not an Array[Byte]")
        }
      case Schema.Type.INT =>
        data match {
          case null                    => null
          case data: java.lang.Integer => Serdes.Integer().serializer().serialize(topic, data)
          case _                       => sys.error("not a java.lang.Integer")
        }
      case Schema.Type.LONG =>
        data match {
          case null                 => null
          case data: java.lang.Long => Serdes.Long().serializer().serialize(topic, data)
          case _                    => sys.error("not a java.lang.Long")
        }
      case Schema.Type.FLOAT =>
        data match {
          case null                  => null
          case data: java.lang.Float => Serdes.Float().serializer().serialize(topic, data)
          case _                     => sys.error("not a java.lang.Float")
        }
      case Schema.Type.DOUBLE =>
        data match {
          case null                   => null
          case data: java.lang.Double => Serdes.Double().serializer().serialize(topic, data)
          case _                      => sys.error("not a java.lang.Double")
        }
      case Schema.Type.BOOLEAN =>
        data match {
          case null                    => null
          case data: java.lang.Boolean => Serdes.Boolean().serializer().serialize(topic, data)
          case _                       => sys.error("not a boolean")
        }
      case us => sys.error(s"unsupported schema: ${us.toString}")
    }

  private val key_serialize: AnyRef => Array[Byte] = getEncoder(pair.key.rawSchema())
  private val val_serialize: AnyRef => Array[Byte] = getEncoder(pair.value.rawSchema())
  
  /** @param gr
    *   a GenericRecord of NJConsumerRecord
    * @return
    */
  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(topic, key_serialize(gr.get("key")), val_serialize(gr.get("value")))
}
