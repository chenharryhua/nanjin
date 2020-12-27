package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{SStreamConfig, SparkSStream}
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZoneId

final class SparKafka[F[_], K, V](
  val topic: KafkaTopic[F, K, V],
  val cfg: SKConfig,
  val sparkSession: SparkSession
) extends Serializable {

  private def updateCfg(f: SKConfig => SKConfig): SparKafka[F, K, V] =
    new SparKafka[F, K, V](topic, f(cfg), sparkSession)

  def withZoneId(zoneId: ZoneId): SparKafka[F, K, V]         = updateCfg(_.withZoneId(zoneId))
  def withTimeRange(tr: NJDateTimeRange): SparKafka[F, K, V] = updateCfg(_.withTimeRange(tr))

  val params: SKParams = cfg.evalConfig

  def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy, sparkSession).map(crRdd)

  def fromDisk: CrRdd[F, K, V] =
    crRdd(loaders.rdd.objectFile[OptionalKV[K, V]](params.replayPath, sparkSession))

  /** shorthand
    */
  def dump(implicit F: Concurrent[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use(blocker =>
      fromKafka.flatMap(cr => cr.save.objectFile(params.replayPath).overwrite.run(blocker) *> cr.count))

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.upload

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.count

  def load: LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](this)

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd, cfg, sparkSession)

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    new CrDS(topic, ate.normalizeDF(df).dataset, ate, cfg)
  }

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] = new PrRdd[F, K, V](topic, rdd, cfg)

  /** structured stream
    */

  def sstream[A](f: OptionalKV[K, V] => A, ate: AvroTypedEncoder[A])(implicit sync: Sync[F]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic, ate, sparkSession)(f),
      SStreamConfig(params.timeRange).withCheckpointBuilder(fmt =>
        s"./data/checkpoint/sstream/kafka/${topic.topicName.value}/${fmt.format}/"))

  def sstream(implicit tek: TypedEncoder[K], tev: TypedEncoder[V], F: Sync[F]): SparkSStream[F, OptionalKV[K, V]] = {
    val ate = OptionalKV.ate(topic.topicDef)
    sstream(identity, ate)
  }
}
