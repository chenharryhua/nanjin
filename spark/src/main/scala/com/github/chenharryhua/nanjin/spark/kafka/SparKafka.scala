package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{KafkaCrSStream, SStreamConfig, SparkSStream}
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
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

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate: AvroTypedEncoder[OptionalKV[K, V]] = OptionalKV.ate(topic.topicDef)
    new CrDS(topic, ate.normalizeDF(df).dataset, ate, cfg)
  }

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] = new PrRdd[F, K, V](topic, rdd, cfg)

  /** structured stream
    */

  def sstream[A](f: OptionalKV[K, V] => A, ate: AvroTypedEncoder[A])(implicit
    sync: Sync[F]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic, ate)(f),
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafka/${topic.topicName.value}"))(ate.typedEncoder)

  def sstream(implicit
    sync: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): KafkaCrSStream[F, K, V] = {
    val ate: AvroTypedEncoder[OptionalKV[K, V]] = OptionalKV.ate(topic.topicDef)
    new KafkaCrSStream[F, K, V](
      sk.kafkaSStream[F, K, V, OptionalKV[K, V]](topic, ate)(identity),
      SStreamConfig(params.timeRange, params.showDs)
        .withCheckpointAppend(s"kafkacr/${topic.topicName.value}"))
  }
}
