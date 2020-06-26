package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
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

  override def params: SKParams = cfg.evalConfig

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

  def dismissNulls: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.dismissNulls, topic, cfg)

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

  // dump java object
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val pathStr = params.replayPath(topic.topicName)
      fileSink(blocker).delete(pathStr) >> F.delay(rdd.saveAsObjectFile(pathStr))
    }

  //actions
  def saveJson(pathStr: String)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    ek: JsonEncoder[K],
    ev: JsonEncoder[V]): F[Unit] =
    Blocker[F].use { blocker =>
      stream.through(fileSink(blocker).json(pathStr)).compile.drain
    }

  def save(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] = {
    import topic.topicDef.{avroKeyEncoder, avroValEncoder}
    Blocker[F].use { blocker =>
      val fmt  = params.fileFormat
      val path = params.pathBuilder(topic.topicName, fmt)
      val data = rdd.persist()
      val run: Stream[F, Unit] = fmt match {
        case NJFileFormat.Avro       => data.stream[F].through(fileSink(blocker).avro(path))
        case NJFileFormat.AvroBinary => data.stream[F].through(fileSink(blocker).binary(path))
        case NJFileFormat.Jackson    => data.stream[F].through(fileSink(blocker).jackson(path))
        case NJFileFormat.Parquet    => data.stream[F].through(fileSink(blocker).parquet(path))
        case NJFileFormat.JavaObject => data.stream[F].through(fileSink(blocker).javaObject(path))
      }
      run.compile.drain.as(data.count) <* F.delay(data.unpersist())
    }
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
