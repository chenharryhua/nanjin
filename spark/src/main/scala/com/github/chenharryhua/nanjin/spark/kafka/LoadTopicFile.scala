package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Decoder
import io.circe.Decoder as JsonDecoder
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], ss: SparkSession)(implicit
  F: Sync[F])
    extends Serializable {

  private val ack: NJAvroCodec[K] = topic.topicDef.rawSerdes.keySerde.avroCodec
  private val acv: NJAvroCodec[V] = topic.topicDef.rawSerdes.valSerde.avroCodec

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv).avroDecoder

  def avro(path: NJPath): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.avro[NJConsumerRecord[K, V]](path, ss, decoder))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }

  def parquet(path: NJPath): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.parquet[NJConsumerRecord[K, V]](path, ss, decoder))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }

  def jackson(path: NJPath): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.jackson[NJConsumerRecord[K, V]](path, ss, decoder))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }

  def binAvro(path: NJPath): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, ss, decoder))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }

  def circe(path: NJPath)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]]): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }

  def objectFile(path: NJPath): CrRdd[F, K, V] = {
    val frdd = F.blocking(loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss))
    new CrRdd[F, K, V](frdd, ack, acv, ss)
  }
}
