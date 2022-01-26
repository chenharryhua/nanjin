package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Decoder
import frameless.TypedEncoder
import fs2.Stream
import io.circe.Decoder as JsonDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends Serializable {

  def avro(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.avro[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def parquet(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.parquet[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def json(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.json[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def jackson(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.jackson[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def binAvro(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.binAvro[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def circe(path: NJPath)(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V],
    jdk: JsonDecoder[K],
    jdv: JsonDecoder[V],
    F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.circe[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  def objectFile(path: NJPath)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.objectFile[NJConsumerRecord[K, V]](path, ate, ss)
      new CrDS(tds, topic, cfg, tek, tev)
    }

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroDecoder

  object rdd {

    def avro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def jackson(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def binAvro(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](path, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def circe(path: NJPath)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]], F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](path, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def objectFile(path: NJPath)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](path, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }
  }

  object stream {
    private val hadoopConfiguration: Configuration = ss.sparkContext.hadoopConfiguration
    private val params: SKParams                   = cfg.evalConfig

    def circe(
      path: NJPath)(implicit F: Sync[F], jdk: JsonDecoder[K], jdv: JsonDecoder[V]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.circe[F, NJConsumerRecord[K, V]](path, hadoopConfiguration, params.loadParams.byteBuffer)

    def jackson(path: NJPath)(implicit F: Async[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream
        .jackson[F, NJConsumerRecord[K, V]](path, decoder, hadoopConfiguration, params.loadParams.byteBuffer)

    def avro(path: NJPath)(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.avro[F, NJConsumerRecord[K, V]](path, decoder, hadoopConfiguration, params.loadParams.chunkSize)
  }
}
