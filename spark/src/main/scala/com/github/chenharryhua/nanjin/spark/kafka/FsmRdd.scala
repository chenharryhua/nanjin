package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.pipes.{
  AvroSerialization,
  CirceSerialization,
  GenericRecordSerialization
}
import com.github.chenharryhua.nanjin.spark.{hadoop, RddExt}
import com.sksamuel.avro4s.AvroSchema
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Queue
import scala.reflect.ClassTag

final class FsmRdd[F[_], K, V](
  val rdd: RDD[NJConsumerRecord[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig)(implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmRdd[F, K, V]] {

  import topic.topicDef.{avroKeyEncoder, avroValEncoder}

  override def params: SKParams = SKConfigF.evalConfig(cfg)

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd, topic, f(cfg))

  //transformation

  def kafkaPartition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.filter(_.partition === num), topic, cfg)

  def sorted: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.sortBy(identity), topic, cfg)

  def descending: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.sortBy(identity, ascending = false), topic, cfg)

  def repartition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.repartition(num), topic, cfg)

  def bimapTo[K2, V2](other: KafkaTopic[F, K2, V2])(k: K => K2, v: V => V2): FsmRdd[F, K2, V2] =
    new FsmRdd[F, K2, V2](rdd.map(_.bimap(k, v)), other, cfg)

  def flatMapTo[K2, V2](other: KafkaTopic[F, K2, V2])(
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]]): FsmRdd[F, K2, V2] =
    new FsmRdd[F, K2, V2](rdd.flatMap(f), other, cfg)

  // out of FsmRdd

  def count: Long = rdd.count()

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords(TypedDataset.create(rdd).dataset, topic, cfg)

  def stream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    rdd.stream[F]

  def source(implicit F: ConcurrentEffect[F]): Source[NJConsumerRecord[K, V], NotUsed] =
    rdd.source[F]

  def malOrdered(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    stream.sliding(2).mapFilter {
      case Queue(a, b) => if (a.timestamp <= b.timestamp) None else Some(a)
    }

  // rdd
  def values(implicit ev: ClassTag[V]): RDD[V] = rdd.flatMap(_.value)
  def keys(implicit ev: ClassTag[K]): RDD[K]   = rdd.flatMap(_.key)

  // save
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val pathStr = params.replayPath(topic.topicName)
      hadoop(blocker).delete(pathStr) >> F.delay(rdd.saveAsObjectFile(pathStr))
    }

  def saveJson(pathStr: String)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    ek: JsonEncoder[K],
    ev: JsonEncoder[V]): F[Unit] =
    Blocker[F].use { blocker =>
      val circe = new CirceSerialization[F, NJConsumerRecord[K, V]]
      val sink  = hadoop(blocker).sink(pathStr)
      stream.through(circe.serialize).through(sink).compile.drain
    }

  def save(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use { blocker =>
      val fmt  = params.fileFormat
      val path = params.pathBuilder(topic.topicName, fmt)
      val h    = hadoop(blocker)
      val as   = new AvroSerialization[F, NJConsumerRecord[K, V]](blocker)
      val data = rdd.persist()
      val run: Stream[F, Unit] = fmt match {
        case NJFileFormat.Avro    => data.stream.through(as.toData).through(h.sink(path))
        case NJFileFormat.Jackson => data.stream.through(as.toByteJson).through(h.sink(path))
        case NJFileFormat.Parquet =>
          val gr = new GenericRecordSerialization[F, NJConsumerRecord[K, V]]
          data.stream
            .through(gr.serialize)
            .through(h.parquetSink(path, topic.topicDef.schemaFor.schema))
      }
      run.compile.drain.as(data.count) <* F.delay(data.unpersist())
    }

  // pipe
  def pipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    stream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(otherTopic, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    pipeTo(topic)

}
