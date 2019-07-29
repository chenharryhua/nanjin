package com.github.chenharryhua.nanjin.sparkafka

import java.util

import com.github.chenharryhua.nanjin.kafka.KafkaAvroSerde
import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer
final case class AvroDeserializerProps[A: Decoder: Encoder](
  schemaRegistry: String,
  schema: String) {

  val props: Map[String, Object] = Map(
    "schema.registry.url" -> schemaRegistry,
    "nanjin.avro.schema" -> schema,
    "nanjin.avro.decoder" -> Decoder[A],
    "nanjin.avro.encoder" -> Encoder[A]
  )
}

final class SparkFriendlyAvroDeserializer[A] extends Deserializer[A] {
  private[this] var avro: KafkaAvroSerde[A] = _
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    println(s"---------------$configs")
    val sr       = configs.get("schema.registry.url").asInstanceOf[String]
    val srclient = new CachedSchemaRegistryClient(sr, 100)
    val sm       = configs.get("schema").asInstanceOf[String]
    val de       = configs.get("decoder").asInstanceOf[Decoder[A]]
    val en       = configs.get("encoder").asInstanceOf[Encoder[A]]
    def rf: RecordFormat[A] = new RecordFormat[A] {
      val schema: Schema                    = new Schema.Parser().parse(sm)
      private val fromRecord: FromRecord[A] = (record: GenericRecord) => de.decode(record, schema)
      private val toRecord: ToRecord[A] = (t: A) =>
        en.encode(t, schema) match {
          case record: Record => record
          case output         => sys.error(s"Cannot marshall an instance of $t to a Record (was $output)")
        }
      override def from(record: GenericRecord): A = fromRecord.from(record)
      override def to(t: A): Record               = toRecord.to(t)
    }
    avro = new KafkaAvroSerde[A](rf, srclient)
    super.configure(configs, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): A =
    avro.deserializer.deserialize(topic, data).value
}
