package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{KafkaCrSStream, SStreamConfig, SparkSStream}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class SparKafka[F[_], K, V](
  val topic: KafkaTopic[F, K, V],
  val sparkSession: SparkSession,
  val cfg: SKConfig
) extends SparKafkaUpdateParams[SparKafka[F, K, V]] {

  implicit private val ss: SparkSession = sparkSession

  val avroCodec: AvroCodec[OptionalKV[K, V]] =
    OptionalKV.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec)

  def ate(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[OptionalKV[K, V]] = {
    implicit val teo: TypedEncoder[OptionalKV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder(avroCodec)
  }

  def kate(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryK[K, V]] = {
    implicit val teo: TypedEncoder[CompulsoryK[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder(
      CompulsoryK.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec))
  }

  def vate(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryV[K, V]] = {
    implicit val teo: TypedEncoder[CompulsoryV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder(
      CompulsoryV.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec))
  }

  def kvate(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryKV[K, V]] = {
    implicit val teo: TypedEncoder[CompulsoryKV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder(
      CompulsoryKV.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec))
  }

  override def withParamUpdate(f: SKConfig => SKConfig): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic, sparkSession, f(cfg))

  def withTopicName(tn: String): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic.withTopicName(tn), sparkSession, cfg)

  override val params: SKParams = cfg.evalConfig

  def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy).map(crRdd)

  def fromDisk: CrRdd[F, K, V] =
    crRdd(loaders.rdd.objectFile[OptionalKV[K, V]](params.replayPath))

  /** shorthand
    */
  def dump(implicit F: Concurrent[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use(blocker =>
      fromKafka.flatMap(cr =>
        cr.save.objectFile(params.replayPath).overwrite.run(blocker) *> cr.count))

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.upload

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.count

  def pipeTo(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  def load: KafkaLoadFile[F, K, V] = new KafkaLoadFile[F, K, V](this)

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] = new CrRdd[F, K, V](topic, rdd, cfg)

  def crDS(tds: TypedDataset[OptionalKV[K, V]])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): CrDS[F, K, V] = new CrDS(topic, tds.dataset, ate, cfg)

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] =
    new CrDS(topic, ate.normalizeDF(df).dataset, ate, cfg)

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] = new PrRdd[F, K, V](topic, rdd, cfg)

  /** structured stream
    */

  def sstream[A](f: OptionalKV[K, V] => A)(implicit
    sync: Sync[F],
    encoder: TypedEncoder[A]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic)(f).dataset,
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafka/${topic.topicName.value}"))

  def sstream(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): KafkaCrSStream[F, K, V] = {
    implicit val te: TypedEncoder[OptionalKV[K, V]] = shapeless.cachedImplicit
    new KafkaCrSStream[F, K, V](
      sk.kafkaSStream[F, K, V, OptionalKV[K, V]](topic)(identity).dataset,
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafkacr/${topic.topicName.value}"))
  }
}
