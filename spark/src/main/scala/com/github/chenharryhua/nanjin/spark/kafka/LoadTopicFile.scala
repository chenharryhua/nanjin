package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder
import frameless.TypedEncoder
import fs2.Stream
import io.circe.{Decoder => JsonDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

final class LoadTopicFile[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends Serializable {

  def avro(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.avro[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def parquet(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.parquet[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def json(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.json[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def jackson(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.jackson[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def binAvro(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.binAvro[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def circe(pathStr: String)(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V],
    ev: JsonDecoder[OptionalKV[K, V]]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.circe[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  def objectFile(pathStr: String)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    val tds = loaders.objectFile[OptionalKV[K, V]](pathStr, ate, ss)
    new CrDS(tds.dataset, topic, cfg, tek, tev)
  }

  private val decoder: Decoder[OptionalKV[K, V]] = OptionalKV.avroCodec(topic.topicDef).avroDecoder

  object rdd {

    def avro(pathStr: String): CrRdd[F, K, V] = {
      val rdd = loaders.rdd.avro[OptionalKV[K, V]](pathStr, decoder, ss)
      new CrRdd[F, K, V](rdd, topic, cfg, ss)
    }

    def jackson(pathStr: String): CrRdd[F, K, V] = {
      val rdd = loaders.rdd.jackson[OptionalKV[K, V]](pathStr, decoder, ss)
      new CrRdd[F, K, V](rdd, topic, cfg, ss)
    }

    def binAvro(pathStr: String): CrRdd[F, K, V] = {
      val rdd = loaders.rdd.binAvro[OptionalKV[K, V]](pathStr, decoder, ss)
      new CrRdd[F, K, V](rdd, topic, cfg, ss)
    }

    def circe(pathStr: String)(implicit ev: JsonDecoder[OptionalKV[K, V]]): CrRdd[F, K, V] = {
      val rdd = loaders.rdd.circe[OptionalKV[K, V]](pathStr, ss)
      new CrRdd[F, K, V](rdd, topic, cfg, ss)
    }

    def objectFile(pathStr: String): CrRdd[F, K, V] = {
      val rdd = loaders.rdd.objectFile[OptionalKV[K, V]](pathStr, ss)
      new CrRdd[F, K, V](rdd, topic, cfg, ss)
    }
  }

  object stream {
    private val hadoopConfiguration: Configuration = ss.sparkContext.hadoopConfiguration

    def circe(pathStr: String, blocker: Blocker)(implicit
      cs: ContextShift[F],
      F: Sync[F],
      ev: JsonDecoder[OptionalKV[K, V]]): Stream[F, OptionalKV[K, V]] =
      loaders.stream.circe[F, OptionalKV[K, V]](pathStr, blocker, hadoopConfiguration)

    def jackson(pathStr: String, blocker: Blocker)(implicit
      cs: ContextShift[F],
      F: ConcurrentEffect[F]): Stream[F, OptionalKV[K, V]] =
      loaders.stream.jackson[F, OptionalKV[K, V]](pathStr, decoder, blocker, hadoopConfiguration)

    def avro(pathStr: String, blocker: Blocker)(implicit cs: ContextShift[F], F: Sync[F]): Stream[F, OptionalKV[K, V]] =
      loaders.stream.avro[F, OptionalKV[K, V]](pathStr, decoder, blocker, hadoopConfiguration)
  }
}
