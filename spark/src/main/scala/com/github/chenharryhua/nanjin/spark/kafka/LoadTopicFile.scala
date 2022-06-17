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

final class LoadTopicFile[F[_], K, V] private[kafka] (
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig,
  ss: SparkSession)
    extends Serializable {

  private val ack: NJAvroCodec[K] = topic.topicDef.rawSerdes.keySerde.avroCodec
  private val acv: NJAvroCodec[V] = topic.topicDef.rawSerdes.valSerde.avroCodec

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv).avroDecoder

  def avro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](path, decoder, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }

  def parquet(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.parquet[NJConsumerRecord[K, V]](path, decoder, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }

  def jackson(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](path, decoder, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }

  def binAvro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, decoder, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }

  def circe(path: NJPath)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]], F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }

  def objectFile(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking {
      val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss)
      new CrRdd[F, K, V](rdd, ack, acv, cfg, ss)
    }
}
