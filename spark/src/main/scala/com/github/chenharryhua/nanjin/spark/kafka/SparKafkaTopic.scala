package com.github.chenharryhua.nanjin.spark.kafka

import cats.Foldable
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, Sync, Timer}
import cats.syntax.bifunctor._
import cats.syntax.foldable._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.dstream.CrDStream
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{SStreamConfig, SparkSStream}
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategy

import java.time.LocalDate

final class SparKafkaTopic[F[_], K, V](val topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends Serializable {

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    NJConsumerRecord.ate(topic.topicDef)

  private def updateCfg(f: SKConfig => SKConfig): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](topic, f(cfg), ss)

  def withStartTime(str: String): SparKafkaTopic[F, K, V]                 = updateCfg(_.withStartTime(str))
  def withEndTime(str: String): SparKafkaTopic[F, K, V]                   = updateCfg(_.withEndTime(str))
  def withOneDay(ld: LocalDate): SparKafkaTopic[F, K, V]                  = updateCfg(_.withOneDay(ld))
  def withLocationStrategy(ls: LocationStrategy): SparKafkaTopic[F, K, V] = updateCfg(_.withLocationStrategy(ls))

  val params: SKParams = cfg.evalConfig

  def fromKafka(implicit F: Effect[F]): CrRdd[F, K, V] =
    crRdd(sk.kafkaBatch(topic, params.timeRange, params.locationStrategy, ss))

  def fromDisk: CrRdd[F, K, V] =
    crRdd(loaders.rdd.objectFile[NJConsumerRecord[K, V]](params.replayPath, ss))

  /** shorthand
    */
  def dump(implicit F: Effect[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use(blocker => fromKafka.save.objectFile(params.replayPath).overwrite.run(blocker))

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    fromDisk.prRdd.noMeta.upload.map(_ => print(".")).compile.drain

  def countKafka(implicit F: Effect[F]): F[Long] = fromKafka.count
  def countDisk(implicit F: Sync[F]): F[Long]    = fromDisk.count

  def load: LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, cfg, ss)

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, topic, cfg, ss)

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] =
    new CrDS(ate.normalizeDF(df).dataset, topic, cfg, tek, tev)

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] = new PrRdd[F, K, V](rdd, topic, cfg)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    prRdd(ss.sparkContext.parallelize(list.toList))

  /** DStream
    */
  def dstream(sc: StreamingContext): CrDStream[K, V] =
    new CrDStream[K, V](sk.kafkaDStream(topic, sc, params.locationStrategy))

  /** structured stream
    */

  def sstream[A](f: NJConsumerRecord[K, V] => A, ate: AvroTypedEncoder[A])(implicit sync: Sync[F]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic, ate, ss)(f),
      SStreamConfig(params.timeRange).withCheckpointBuilder(fmt =>
        s"./data/checkpoint/sstream/kafka/${topic.topicName.value}/${fmt.format}/"))

  def sstream(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V],
    F: Sync[F]): SparkSStream[F, NJConsumerRecord[K, V]] =
    sstream(identity, ate)

  def jsonStream(implicit
    jek: JsonEncoder[K],
    jev: JsonEncoder[V],
    jdk: JsonDecoder[K],
    jdv: JsonDecoder[V],
    F: Sync[F]): SparkSStream[F, NJConsumerRecord[KJson[K], KJson[V]]] = {
    import com.github.chenharryhua.nanjin.spark.injection.kjsonInjection
    val ack: AvroCodec[KJson[K]]             = AvroCodec[KJson[K]]
    val acv: AvroCodec[KJson[V]]             = AvroCodec[KJson[V]]
    implicit val kte: TypedEncoder[KJson[K]] = shapeless.cachedImplicit
    implicit val vte: TypedEncoder[KJson[V]] = shapeless.cachedImplicit

    val ate: AvroTypedEncoder[NJConsumerRecord[KJson[K], KJson[V]]] = NJConsumerRecord.ate[KJson[K], KJson[V]](ack, acv)

    sstream[NJConsumerRecord[KJson[K], KJson[V]]](_.bimap(KJson(_), KJson(_)), ate)
  }
}
