package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder
import frameless.TypedEncoder
import fs2.Stream
import io.circe.Decoder as JsonDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends Serializable {

  def avro(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.avro[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def parquet(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.parquet[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def json(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.json[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def jackson(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.jackson[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def binAvro(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.binAvro[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def circe(pathStr: String)(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V],
    ev: JsonDecoder[NJConsumerRecord[K, V]],
    F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.circe[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  def objectFile(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): F[CrDS[F, K, V]] =
    F.blocking {
      val ate = NJConsumerRecord.ate(topic.topicDef)
      val tds = loaders.objectFile[NJConsumerRecord[K, V]](pathStr, ate, ss)
      new CrDS(tds.dataset, topic, cfg, tek, tev)
    }

  private val decoder: Decoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroDecoder

  object rdd {

    def avro(pathStr: String)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.avro[NJConsumerRecord[K, V]](pathStr, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def jackson(pathStr: String)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.jackson[NJConsumerRecord[K, V]](pathStr, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def binAvro(pathStr: String)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.binAvro[NJConsumerRecord[K, V]](pathStr, decoder, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def circe(pathStr: String)(implicit ev: JsonDecoder[NJConsumerRecord[K, V]], F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.circe[NJConsumerRecord[K, V]](pathStr, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }

    def objectFile(pathStr: String)(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
      F.blocking {
        val rdd = loaders.rdd.objectFile[NJConsumerRecord[K, V]](pathStr, ss)
        new CrRdd[F, K, V](rdd, topic, cfg, ss)
      }
  }

  object stream {
    private val hadoopConfiguration: Configuration = ss.sparkContext.hadoopConfiguration

    def circe(pathStr: String)(implicit
      F: Sync[F],
      ev: JsonDecoder[NJConsumerRecord[K, V]]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.circe[F, NJConsumerRecord[K, V]](pathStr, hadoopConfiguration)

    def jackson(pathStr: String)(implicit F: Async[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.jackson[F, NJConsumerRecord[K, V]](pathStr, decoder, hadoopConfiguration)

    def avro(pathStr: String)(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
      loaders.stream.avro[F, NJConsumerRecord[K, V]](pathStr, decoder, hadoopConfiguration)
  }
}
