package com.github.chenharryhua.nanjin.spark.kafka

import cats.{Endo, Foldable}
import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{PathSegment, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJConsumerRecordWithError, NJProducerRecord}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJAvroCodec}
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerde, CirceSerde, JacksonSerde, JavaObjectSerde}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.dstream.{AvroDStreamSink, SDConfig}
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.spark.sstream.{SStreamConfig, SparkSStream}
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import fs2.kafka.ProducerResult
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategy

import java.time.LocalDate

final class SparKafkaTopic[F[_], K, V](val sparkSession: SparkSession, val topic: KafkaTopic[F, K, V], cfg: SKConfig)
    extends UpdateConfig[SKConfig, SparKafkaTopic[F, K, V]] with Serializable {
  override val toString: String = topic.topicName.value

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    AvroTypedEncoder(topic.topicDef)

  val crCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
    NJConsumerRecord.avroCodec(topic.topicDef.rawSerdes.keySerde.avroCodec, topic.topicDef.rawSerdes.valSerde.avroCodec)
  val prCodec: NJAvroCodec[NJProducerRecord[K, V]] =
    NJProducerRecord.avroCodec(topic.topicDef.rawSerdes.keySerde.avroCodec, topic.topicDef.rawSerdes.valSerde.avroCodec)

  object pipes {
    object circe {
      def toBytes(
        isKeepNull: Boolean)(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): Pipe[F, NJConsumerRecord[K, V], Byte] =
        CirceSerde.toBytes[F, NJConsumerRecord[K, V]](isKeepNull)
      def fromBytes(implicit
        F: RaiseThrowable[F],
        jk: JsonDecoder[K],
        jv: JsonDecoder[V]): Pipe[F, Byte, NJConsumerRecord[K, V]] =
        CirceSerde.fromBytes[F, NJConsumerRecord[K, V]]
    }

    object jackson {
      val toBytes: Pipe[F, NJConsumerRecord[K, V], Byte] =
        _.mapChunks(_.map(crCodec.toRecord)).through(JacksonSerde.toBytes[F](crCodec.schema))

      def fromBytes(implicit F: Async[F]): Pipe[F, Byte, NJConsumerRecord[K, V]] =
        JacksonSerde.fromBytes[F](crCodec.schema).andThen(_.mapChunks(_.map(crCodec.fromRecord)))
    }

    object binAvro {
      val toBytes: Pipe[F, NJConsumerRecord[K, V], Byte] =
        _.mapChunks(_.map(crCodec.toRecord)).through(BinaryAvroSerde.toBytes[F](crCodec.schema))
      def fromBytes(implicit F: Async[F]): Pipe[F, Byte, NJConsumerRecord[K, V]] =
        BinaryAvroSerde.fromBytes[F](crCodec.schema).andThen(_.mapChunks(_.map(crCodec.fromRecord)))
    }

    object javaObj {
      val toBytes: Pipe[F, NJConsumerRecord[K, V], Byte] = JavaObjectSerde.toBytes[F, NJConsumerRecord[K, V]]
      def fromBytes(implicit ce: Async[F]): Pipe[F, Byte, NJConsumerRecord[K, V]] =
        JavaObjectSerde.fromBytes[F, NJConsumerRecord[K, V]]
    }

    object genericRecord {
      val toRecord: Pipe[F, NJConsumerRecord[K, V], GenericRecord]   = _.mapChunks(_.map(crCodec.toRecord))
      val fromRecord: Pipe[F, GenericRecord, NJConsumerRecord[K, V]] = _.mapChunks(_.map(crCodec.fromRecord))
    }
  }

  override def updateConfig(f: Endo[SKConfig]): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](sparkSession, topic, f(cfg))

  def withStartTime(str: String): SparKafkaTopic[F, K, V]                 = updateConfig(_.startTime(str))
  def withEndTime(str: String): SparKafkaTopic[F, K, V]                   = updateConfig(_.endTime(str))
  def withOneDay(ld: LocalDate): SparKafkaTopic[F, K, V]                  = updateConfig(_.timeRangeOneDay(ld))
  def withTimeRange(tr: NJDateTimeRange): SparKafkaTopic[F, K, V]         = updateConfig(_.timeRange(tr))
  def withLocationStrategy(ls: LocationStrategy): SparKafkaTopic[F, K, V] = updateConfig(_.locationStrategy(ls))

  val params: SKParams     = cfg.evalConfig
  val segment: PathSegment = PathSegment.unsafeFrom(topicName.value)

  def rawKafka(implicit F: Sync[F]): F[RDD[NJConsumerRecordWithError[K, V]]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy, sparkSession)

  def fromKafka(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    rawKafka.map(rdd => crRdd(rdd.map(_.toNJConsumerRecord)))

  def fromDisk(implicit F: Sync[F]): F[CrRdd[F, K, V]] =
    F.blocking(crRdd(loaders.rdd.objectFile[NJConsumerRecord[K, V]](params.replayPath, sparkSession)))

  /** shorthand
    */
  def dump(implicit F: Sync[F]): F[Unit] =
    fromKafka.flatMap(_.save.objectFile(params.replayPath).overwrite.run)

  def dumpToday(implicit F: Sync[F]): F[Unit] = withOneDay(LocalDate.now()).dump

  def replay(listener: ProducerResult[K, V] => F[Unit])(implicit F: Async[F]): F[Unit] =
    Stream
      .force(
        fromDisk.map(
          _.prRdd.noMeta.producerRecords(topicName, 1000).through(topic.fs2Channel.producerPipe).evalMap(listener)))
      .compile
      .drain

  /** replay last num records
    */
  def replay(num: Long)(implicit F: Async[F]): F[Unit] =
    Stream
      .force(fromDisk.map(
        _.prRdd.descendTimestamp.noMeta.producerRecords(topicName, 1).take(num).through(topic.fs2Channel.producerPipe)))
      .compile
      .drain

  def replay(implicit F: Async[F]): F[Unit] = replay(_ => F.unit)

  def countKafka(implicit F: Sync[F]): F[Long] = fromKafka.flatMap(_.count)
  def countDisk(implicit F: Sync[F]): F[Long]  = fromDisk.flatMap(_.count)

  def load: LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, cfg, sparkSession)

  val avroKeyCodec: NJAvroCodec[K] = topic.topicDef.rawSerdes.keySerde.avroCodec
  val avroValCodec: NJAvroCodec[V] = topic.topicDef.rawSerdes.valSerde.avroCodec

  /** rdd and dataset
    */
  def crRdd(rdd: RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, avroKeyCodec, avroValCodec, cfg, sparkSession)

  def crDS(df: DataFrame)(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] =
    new CrDS(ate.normalizeDF(df), cfg, avroKeyCodec, avroValCodec, tek, tev)

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, prCodec, cfg)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    prRdd(sparkSession.sparkContext.parallelize(list.toList))

  /** DStream
    */
  def dstream(listener: NJConsumerRecordWithError[K, V] => Unit)(implicit
    F: Async[F]): Kleisli[F, StreamingContext, AvroDStreamSink[NJConsumerRecord[K, V]]] =
    Kleisli((sc: StreamingContext) =>
      sk.kafkaDStream(topic, sc, params.locationStrategy, listener)
        .map(ds => new AvroDStreamSink(ds, crCodec.avroEncoder, SDConfig(params.timeRange.zoneId))))

  def dstream(implicit F: Async[F]): Kleisli[F, StreamingContext, AvroDStreamSink[NJConsumerRecord[K, V]]] =
    dstream(_ => ())

  /** structured stream
    */

  def sstream[A](f: NJConsumerRecord[K, V] => A, ate: AvroTypedEncoder[A]): SparkSStream[F, A] =
    new SparkSStream[F, A](
      sk.kafkaSStream[F, K, V, A](topic, ate, sparkSession)(f),
      SStreamConfig(params.timeRange.zoneId).checkpointBuilder(fmt =>
        NJPath("./data/checkpoint") / "sstream" / "kafka" / segment / PathSegment.unsafeFrom(fmt.format)))

  def sstream(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): SparkSStream[F, NJConsumerRecord[K, V]] =
    sstream(identity[NJConsumerRecord[K, V]], ate)

  def jsonStream(implicit
    jek: JsonEncoder[K],
    jev: JsonEncoder[V],
    jdk: JsonDecoder[K],
    jdv: JsonDecoder[V]): SparkSStream[F, NJConsumerRecord[KJson[K], KJson[V]]] = {
    import com.github.chenharryhua.nanjin.spark.injection.kjsonInjection
    val ack: NJAvroCodec[KJson[K]]           = NJAvroCodec[KJson[K]]
    val acv: NJAvroCodec[KJson[V]]           = NJAvroCodec[KJson[V]]
    implicit val kte: TypedEncoder[KJson[K]] = shapeless.cachedImplicit
    implicit val vte: TypedEncoder[KJson[V]] = shapeless.cachedImplicit

    val ate: AvroTypedEncoder[NJConsumerRecord[KJson[K], KJson[V]]] = AvroTypedEncoder[KJson[K], KJson[V]](ack, acv)

    sstream[NJConsumerRecord[KJson[K], KJson[V]]](_.bimap(KJson(_), KJson(_)), ate)
  }
}
