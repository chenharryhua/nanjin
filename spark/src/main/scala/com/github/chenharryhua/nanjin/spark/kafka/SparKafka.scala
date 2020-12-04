package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{KafkaCrSStream, SStreamConfig, SparkSStream}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait SparKafkaUpdateParams[A] extends UpdateParams[SKConfig, A] with Serializable {
  def params: SKParams
}

final class SparKafka[F[_], K, V](
  val topic: KafkaTopic[F, K, V],
  val sparkSession: SparkSession,
  val cfg: SKConfig
) extends SparKafkaUpdateParams[SparKafka[F, K, V]] {

  private val keyCodec: AvroCodec[K] = topic.codec.keySerde.avroCodec
  private val valCodec: AvroCodec[V] = topic.codec.valSerde.avroCodec

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
  def dump(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use(blocker =>
      fromKafka.flatMap(_.save.overwrite.objectFile(params.replayPath).run(blocker)))

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.pipeTo(topic)

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.count

  def pipeTo(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    fromKafka.flatMap(_.pipeTo(other))

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, cfg, keyCodec, valCodec)

  def crRdd(tds: TypedDataset[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](tds.dataset.rdd, cfg, keyCodec, valCodec)

  /** direct stream
    */

  def dstream(sc: StreamingContext): CrDStream[F, K, V] =
    new CrDStream[F, K, V](sk.kafkaDStream[F, K, V](topic, sc, params.locationStrategy), cfg)

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
