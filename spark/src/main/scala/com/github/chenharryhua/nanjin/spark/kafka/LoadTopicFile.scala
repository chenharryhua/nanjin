package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[K, V] private[kafka] (topicDef: TopicDef[K, V], ss: SparkSession)
    extends Serializable {

  private val ack: AvroCodec[K] = topicDef.serdePair.key.avroCodec
  private val acv: AvroCodec[V] = topicDef.serdePair.value.avroCodec

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv)

  def avro(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }

  def parquet(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.parquet[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }

  def jackson(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }

  def binAvro(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }

  def circe(path: Url)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]]): CrRdd[K, V] = {
    val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }

  def objectFile(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss)
    new CrRdd[K, V](rdd, ack, acv, ss)
  }
}
