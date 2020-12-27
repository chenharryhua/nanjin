package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder
import frameless.TypedEncoder
import fs2.Stream
import io.circe.{Decoder => JsonDecoder}
import org.apache.hadoop.conf.Configuration

final class LoadTopicFile[F[_], K, V] private[kafka] (klf: SparKafka[F, K, V]) extends Serializable {

  def avro(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.avro[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def parquet(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.parquet[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def json(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.json[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def jackson(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.jackson[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def binAvro(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.binAvro[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def circe(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ev: JsonDecoder[OptionalKV[K, V]]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.circe[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  def objectFile(pathStr: String)(implicit keyEncoder: TypedEncoder[K], valEncoder: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(klf.topic.topicDef)
    val tds = loaders.objectFile[OptionalKV[K, V]](pathStr, ate, klf.sparkSession)
    new CrDS(klf.topic, tds.dataset, ate, klf.cfg)
  }

  private val decoder: Decoder[OptionalKV[K, V]] = OptionalKV.avroCodec(klf.topic.topicDef).avroDecoder

  object rdd {

    def avro(pathStr: String): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.avro[OptionalKV[K, V]](pathStr, decoder, klf.sparkSession))

    def jackson(pathStr: String): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.jackson[OptionalKV[K, V]](pathStr, decoder, klf.sparkSession))

    def binAvro(pathStr: String): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.binAvro[OptionalKV[K, V]](pathStr, decoder, klf.sparkSession))

    def circe(pathStr: String)(implicit ev: JsonDecoder[OptionalKV[K, V]]): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.circe[OptionalKV[K, V]](pathStr, klf.sparkSession))

    def objectFile(pathStr: String): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.objectFile[OptionalKV[K, V]](pathStr, klf.sparkSession))
  }

  object stream {
    private val hadoopConfiguration: Configuration = klf.sparkSession.sparkContext.hadoopConfiguration

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
