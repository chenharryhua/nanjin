package com.github.chenharryhua.nanjin.spark.kafka

import cats.{Foldable, Monad}
import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.PathSegment
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerde, CirceSerde, JacksonSerde, JavaObjectSerde}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.dstream.AvroDStreamSink
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import fs2.{Pipe, RaiseThrowable}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

final class SparKafkaTopic[F[_], K, V](val sparkSession: SparkSession, val topic: KafkaTopic[F, K, V])
    extends Serializable {
  override val toString: String = topic.topicName.value

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    AvroTypedEncoder(topic.topicDef)

  val avroKeyCodec: NJAvroCodec[K] = topic.topicDef.rawSerdes.keySerde.avroCodec
  val avroValCodec: NJAvroCodec[V] = topic.topicDef.rawSerdes.valSerde.avroCodec

  val crCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
    NJConsumerRecord.avroCodec(
      topic.topicDef.rawSerdes.keySerde.avroCodec,
      topic.topicDef.rawSerdes.valSerde.avroCodec)
  val prCodec: NJAvroCodec[NJProducerRecord[K, V]] =
    NJProducerRecord.avroCodec(
      topic.topicDef.rawSerdes.keySerde.avroCodec,
      topic.topicDef.rawSerdes.valSerde.avroCodec)

  object pipes {
    object circe {
      def toBytes(isKeepNull: Boolean)(implicit
        jk: JsonEncoder[K],
        jv: JsonEncoder[V]): Pipe[F, NJConsumerRecord[K, V], Byte] =
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

  val segment: PathSegment = PathSegment.unsafeFrom(topicName.value)

  private def downloadKafka(dateTimeRange: NJDateTimeRange)(implicit F: Sync[F]): CrRdd[F, K, V] =
    crRdd(F.blocking(sk.kafkaBatch(topic, sparkSession, dateTimeRange).map(_.toNJConsumerRecord)))

  /** download topic according to datetime
    *
    * @param dtr
    *   : datetime
    */
  def fromKafka(dtr: NJDateTimeRange)(implicit F: Sync[F]): CrRdd[F, K, V] =
    downloadKafka(dtr)

  /** download all topic data, up to now
    */
  def fromKafka(implicit F: Sync[F]): CrRdd[F, K, V] =
    fromKafka(NJDateTimeRange(topic.context.settings.zoneId))

  /** download topic according to offset range
    * @param offsets
    *
    * partition -> (start-offset(inclusive), end-offset(exclusive))
    *
    * @return
    *   CrRdd
    */
  def fromKafka(offsets: Map[Int, (Long, Long)])(implicit F: Sync[F]): CrRdd[F, K, V] =
    crRdd(
      topic.shortLiveConsumer
        .use(_.partitionsFor.map { partitions =>
          val topicPartition = partitions.value.map { tp =>
            val ofs = offsets
              .get(tp.partition())
              .flatMap(se => KafkaOffsetRange(KafkaOffset(se._1), KafkaOffset(se._2)))
            tp -> ofs
          }.toMap
          KafkaTopicPartition(topicPartition)
        })
        .flatMap(offsetRange =>
          F.blocking(sk.kafkaBatch(topic, sparkSession, offsetRange).map(_.toNJConsumerRecord))))

  /** load topic data from disk
    */

  def load(implicit F: Sync[F]): LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, sparkSession)

  /** rdd and dataset
    */

  def crRdd(rdd: F[RDD[NJConsumerRecord[K, V]]])(implicit M: Monad[F]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, avroKeyCodec, avroValCodec, sparkSession)

  def emptyCrRdd(implicit M: Monad[F]): CrRdd[F, K, V] = crRdd(
    M.pure(sparkSession.sparkContext.emptyRDD[NJConsumerRecord[K, V]]))

  def prRdd(rdd: F[RDD[NJProducerRecord[K, V]]])(implicit M: Monad[F]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, prCodec)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]])(implicit M: Monad[F]): PrRdd[F, K, V] =
    prRdd(M.pure(sparkSession.sparkContext.parallelize(list.toList)))

  def emptyPrRdd(implicit M: Monad[F]): PrRdd[F, K, V] =
    prRdd(M.pure(sparkSession.sparkContext.emptyRDD[NJProducerRecord[K, V]]))

  /** DStream
    */
  def dstream(implicit F: Async[F]): Kleisli[F, StreamingContext, AvroDStreamSink[NJConsumerRecord[K, V]]] =
    Kleisli((sc: StreamingContext) =>
      sk.kafkaDStream(topic, sc)
        .map(ds => new AvroDStreamSink(ds, crCodec.avroEncoder, root => ldt => root / ldt.toLocalDate)))

//
//  def sstream[A](f: NJConsumerRecord[K, V] => A, te: TypedEncoder[A]): SparkSStream[F, A] =
//    new SparkSStream[F, A](
//      sk.kafkaSStream[F, K, V, A](topic, te, sparkSession)(f),
//      SStreamConfig(params.timeRange.zoneId).checkpointBuilder(fmt =>
//        NJPath("./data/checkpoint") / "sstream" / "kafka" / segment / PathSegment.unsafeFrom(fmt.format)))
//
//  def sstream(implicit
//    @nowarn tek: TypedEncoder[K],
//    @nowarn tev: TypedEncoder[V]): SparkSStream[F, NJConsumerRecord[K, V]] = {
//    val te: TypedEncoder[NJConsumerRecord[K, V]] = shapeless.cachedImplicit
//    sstream[NJConsumerRecord[K, V]](identity[NJConsumerRecord[K, V]], te)
//  }
}
