package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[K: Decoder, V: Decoder] private[kafka] (ss: SparkSession) extends Serializable {

  private val decoder: Decoder[NJConsumerRecord[K, V]] = Decoder[NJConsumerRecord[K, V]]

  def avro(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ss)
  }

  def parquet(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.parquet[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ss)
  }

  def jackson(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ss)
  }

  def binAvro(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, ss, decoder)
    new CrRdd[K, V](rdd, ss)
  }

  def circe(path: Url)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]]): CrRdd[K, V] = {
    val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss)
    new CrRdd[K, V](rdd, ss)
  }

  def objectFile(path: Url): CrRdd[K, V] = {
    val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss)
    new CrRdd[K, V](rdd, ss)
  }
}
