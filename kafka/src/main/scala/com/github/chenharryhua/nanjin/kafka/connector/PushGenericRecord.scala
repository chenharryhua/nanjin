package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.schema.immigrate
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, SerdeSettings, TopicName}
import fs2.kafka.ProducerRecord
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes

import scala.jdk.CollectionConverters.given
import scala.util.{Failure, Success}

final private class PushGenericRecord(
  srClient: SchemaRegistryClient,
  serdeSettings: SerdeSettings,
  topicName: TopicName,
  pair: AvroSchemaPair) {

  private val topic: String = topicName.value

  private def getEncoder(skm: Schema, isKey: Boolean): AnyRef => Array[Byte] = {
    skm.getType match
      case Schema.Type.RECORD =>
        val ser = new KafkaAvroSerializer(srClient)
        ser.configure(serdeSettings.properties.asJava, isKey)
        // java world
        (_: AnyRef) match {
          case null              => null
          case gr: GenericRecord =>
            immigrate(skm, gr).map(ser.serialize(topic, _)) match {
              case Success(value) => value
              case Failure(ex)    => throw ex // scalafix:ok
            }
          case unknown =>
            sys.error(s"${unknown.getClass.getName} is not a Generic Record")
        }

      case Schema.Type.STRING =>
        val ser = Serdes.String().serializer()
        (_: AnyRef) match
          case null         => null
          case data: String => ser.serialize(topic, data)
          case unknown      => sys.error(s"${unknown.getClass.getName} not a String")

      case Schema.Type.BYTES =>
        val ser = Serdes.ByteArray().serializer()
        (_: AnyRef) match
          case null              => null
          case data: Array[Byte] => ser.serialize(topic, data)
          case unknown           => sys.error(s"${unknown.getClass.getName} not an Array[Byte]")

      case Schema.Type.INT =>
        val ser = Serdes.Integer().serializer()
        (_: AnyRef) match
          case null                    => null
          case data: java.lang.Integer => ser.serialize(topic, data)
          case unknown                 => sys.error(s"${unknown.getClass.getName} not a java.lang.Integer")

      case Schema.Type.LONG =>
        val ser = Serdes.Long().serializer()
        (_: AnyRef) match
          case null                 => null
          case data: java.lang.Long => ser.serialize(topic, data)
          case unknown              => sys.error(s"${unknown.getClass.getName} not a java.lang.Long")

      case Schema.Type.FLOAT =>
        val ser = Serdes.Float().serializer()
        (_: AnyRef) match
          case null                  => null
          case data: java.lang.Float => ser.serialize(topic, data)
          case unknown               => sys.error(s"${unknown.getClass.getName} not a java.lang.Float")

      case Schema.Type.DOUBLE =>
        val ser = Serdes.Double().serializer()
        (_: AnyRef) match
          case null                   => null
          case data: java.lang.Double => ser.serialize(topic, data)
          case unknown                => sys.error(s"${unknown.getClass.getName} is not a java.lang.Double")

      case Schema.Type.BOOLEAN =>
        val ser = Serdes.Boolean().serializer()
        (_: AnyRef) match
          case null                    => null
          case data: java.lang.Boolean => ser.serialize(topic, data)
          case unknown                 => sys.error(s"${unknown.getClass.getName} is not a java.lang.Boolean")

      case us => sys.error(s"unsupported schema: ${us.toString}")
    end match
  }

  private val key_serialize: AnyRef => Array[Byte] = getEncoder(pair.key.rawSchema(), true)
  private val val_serialize: AnyRef => Array[Byte] = getEncoder(pair.value.rawSchema(), false)

  /** @param gr
    *   a GenericRecord of NJConsumerRecord
    * @return
    */
  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(topic, key_serialize(gr.get("key")), val_serialize(gr.get("value")))
}
