package com.github.chenharryhua.nanjin.spark.kafka

import akka.actor.ActorSystem
import cats.Foldable
import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{akkaUpdater, KafkaTopic}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.dstream.{AvroDStreamSink, SDConfig}
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{SStreamConfig, SparkSStream}
import frameless.TypedEncoder
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategy

import java.time.LocalDate

final class SparKafkaTopic[F[_], K, V](val topic: KafkaTopic[F, K, V], cfg: SKConfig, ss: SparkSession)
    extends UpdateConfig[SKConfig, SparKafkaTopic[F, K, V]] with Serializable {
  override val toString: String = topic.topicName.value

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    NJConsumerRecord.ate(topic.topicDef)

  override def updateConfig(f: SKConfig => SKConfig): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](topic, f(cfg), ss)

  def withStartTime(str: String): SparKafkaTopic[F, K, V]                 = updateConfig(_.startTime(str))
  def withEndTime(str: String): SparKafkaTopic[F, K, V]                   = updateConfig(_.endTime(str))
  def withOneDay(ld: LocalDate): SparKafkaTopic[F, K, V]                  = updateConfig(_.timeRangeOneDay(ld))
  def withTimeRange(tr: NJDateTimeRange): SparKafkaTopic[F, K, V]         = updateConfig(_.timeRange(tr))
  def withLocationStrategy(ls: LocationStrategy): SparKafkaTopic[F, K, V] = updateConfig(_.locationStrategy(ls))

  val params: SKParams = cfg.evalConfig

  def rawKafka(implicit F: Sync[F]): F[RDD[NJConsumerRecordWithError[K, V]]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy, ss)

  def fromKafka(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    rawKafka.map(rdd => crRdd(rdd.map(_.toNJConsumerRecord)))

  def fromDisk(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking(crRdd(loaders.rdd.objectFile[NJConsumerRecord[K, V]](params.replayPath, ss)))

  /** shorthand
    */
  def dump(implicit F: Sync[F]): F[Unit] =
    fromKafka.flatMap(_.save.objectFile(params.replayPath).overwrite.run)

  def dumpToday(implicit F: Sync[F]): F[Unit] = withOneDay(LocalDate.now()).dump

  def replay(implicit ce: Async[F]): F[Unit] =
    fromDisk.flatMap(_.prRdd.noMeta.fs2Upload.run.map(_ => print(".")).compile.drain)

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.flatMap(_.count)

  def load: LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, cfg, ss)

  def download(akkaSystem: ActorSystem): KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, ss.sparkContext.hadoopConfiguration, cfg, akkaUpdater.unitConsumer)

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, topic, cfg, ss)

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] =
    new CrDS(ate.normalizeDF(df), topic, cfg, tek, tev)

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, topic, cfg)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    prRdd(ss.sparkContext.parallelize(list.toList))

  /** DStream
    */
  def dstream(implicit F: Async[F]): Kleisli[F, StreamingContext, AvroDStreamSink[NJConsumerRecord[K, V]]] =
    Kleisli((sc: StreamingContext) =>
      sk.kafkaDStream(topic, sc, params.locationStrategy)
        .map(ds =>
          new AvroDStreamSink(
            ds,
            NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder,
            SDConfig(params.timeRange.zoneId))))

  /** structured stream
    */

  def sstream[A](f: NJConsumerRecord[K, V] => A, ate: AvroTypedEncoder[A]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic, ate, ss)(f),
      SStreamConfig(params.timeRange.zoneId).checkpointBuilder(fmt =>
        s"./data/checkpoint/sstream/kafka/${topic.topicName.value}/${fmt.format}/"))

  def sstream(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): SparkSStream[F, NJConsumerRecord[K, V]] =
    sstream(identity[NJConsumerRecord[K, V]](_), ate)

  def jsonStream(implicit
    jek: JsonEncoder[K],
    jev: JsonEncoder[V],
    jdk: JsonDecoder[K],
    jdv: JsonDecoder[V]): SparkSStream[F, NJConsumerRecord[KJson[K], KJson[V]]] = {
    import com.github.chenharryhua.nanjin.spark.injection.kjsonInjection
    val ack: AvroCodec[KJson[K]]             = AvroCodec[KJson[K]]
    val acv: AvroCodec[KJson[V]]             = AvroCodec[KJson[V]]
    implicit val kte: TypedEncoder[KJson[K]] = shapeless.cachedImplicit
    implicit val vte: TypedEncoder[KJson[V]] = shapeless.cachedImplicit

    val ate: AvroTypedEncoder[NJConsumerRecord[KJson[K], KJson[V]]] = NJConsumerRecord.ate[KJson[K], KJson[V]](ack, acv)

    sstream[NJConsumerRecord[KJson[K], KJson[V]]](_.bimap(KJson(_), KJson(_)), ate)
  }
}
