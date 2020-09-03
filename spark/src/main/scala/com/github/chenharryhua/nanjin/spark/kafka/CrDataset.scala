package com.github.chenharryhua.nanjin.spark.kafka

import java.time.LocalDate

import cats.Eq
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{localdateInstances, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  NJProducerRecord,
  OptionalKV
}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{RddFileHoarder, RddPartitionHoarder}
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits._
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.sksamuel.avro4s.SchemaFor

final class CrDataset[F[_], K, V](
  val dataset: Dataset[OptionalKV[K, V]],
  codec: KafkaAvroTypedEncoder[K, V],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[CrDataset[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): CrDataset[F, K, V] =
    new CrDataset[F, K, V](dataset, codec, f(cfg))

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(dataset)(codec.optionalKV)

  override val params: SKParams = cfg.evalConfig

  // transformations
  def bimap[
    K2: TypedEncoder: AvroEncoder: AvroDecoder: SchemaFor,
    V2: TypedEncoder: AvroEncoder: AvroDecoder: SchemaFor](
    k: K => K2,
    v: V => V2): CrDataset[F, K2, V2] = {
    val codec = new KafkaAvroTypedEncoder[K2, V2](
      TypedEncoder[K2],
      TypedEncoder[V2],
      new KafkaAvroCodec[K2, V2](NJAvroCodec[K2], NJAvroCodec[V2]))
    new CrDataset[F, K2, V2](typedDataset.deserialized.map(_.bimap(k, v)).dataset, codec, cfg)
  }

  def flatMap[
    K2: TypedEncoder: AvroEncoder: AvroDecoder: SchemaFor,
    V2: TypedEncoder: AvroEncoder: AvroDecoder: SchemaFor](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): CrDataset[F, K2, V2] = {
    val codec = new KafkaAvroTypedEncoder[K2, V2](
      TypedEncoder[K2],
      TypedEncoder[V2],
      new KafkaAvroCodec[K2, V2](NJAvroCodec[K2], NJAvroCodec[V2]))
    new CrDataset[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, codec, cfg)
  }

  def filter(f: OptionalKV[K, V] => Boolean): CrDataset[F, K, V] =
    new CrDataset[F, K, V](dataset.filter(f), codec, cfg)

  def ascending: CrDataset[F, K, V] = {
    val sd = typedDataset.orderBy(
      typedDataset('timestamp).asc,
      typedDataset('offset).asc,
      typedDataset('partition).asc)
    new CrDataset[F, K, V](sd.dataset, codec, cfg)
  }

  def descending: CrDataset[F, K, V] = {
    val sd = typedDataset.orderBy(
      typedDataset('timestamp).desc,
      typedDataset('offset).desc,
      typedDataset('partition).desc)
    new CrDataset[F, K, V](sd.dataset, codec, cfg)
  }

  def persist: CrDataset[F, K, V] =
    new CrDataset[F, K, V](dataset.persist(), codec, cfg)

  def inRange(dr: NJDateTimeRange): CrDataset[F, K, V] =
    new CrDataset[F, K, V](
      dataset.filter((m: OptionalKV[K, V]) => dr.isInBetween(m.timestamp)),
      codec,
      cfg.withTimeRange(dr))

  def inRange: CrDataset[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrDataset[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // dataset

  def values: TypedDataset[CompulsoryV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryV)(codec.compulsoryV)

  def keys: TypedDataset[CompulsoryK[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryK)(codec.compulsoryK)

  def keyValues: TypedDataset[CompulsoryKV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryKV)(codec.compulsoryKV)

  // investigations:
  def stats: Statistics[F] =
    new Statistics[F](typedDataset.deserialized.map(CRMetaInfo(_)).dataset, cfg)

  def missingData: TypedDataset[CRMetaInfo] =
    inv.missingData(values.deserialized.map(CRMetaInfo(_)))

  def dupRecords: TypedDataset[DupResult] =
    inv.dupRecords(typedDataset.deserialized.map(CRMetaInfo(_)))

  def diff(other: TypedDataset[OptionalKV[K, V]])(implicit
    ke: Eq[K],
    ve: Eq[V]): TypedDataset[DiffResult[K, V]] = {
    implicit val k: TypedEncoder[K] = codec.keyEncoder
    implicit val v: TypedEncoder[V] = codec.valEncoder
    inv.diffDataset(typedDataset, other)
  }

  def diff(
    other: CrDataset[F, K, V])(implicit ke: Eq[K], ve: Eq[V]): TypedDataset[DiffResult[K, V]] =
    diff(other.typedDataset)

  def kvDiff(other: TypedDataset[OptionalKV[K, V]]): TypedDataset[KvDiffResult[K, V]] = {
    implicit val k: TypedEncoder[K] = codec.keyEncoder
    implicit val v: TypedEncoder[V] = codec.valEncoder
    inv.kvDiffDataset(typedDataset, other)
  }

  def kvDiff(other: CrDataset[F, K, V]): TypedDataset[KvDiffResult[K, V]] =
    kvDiff(other.typedDataset)

  def find(f: OptionalKV[K, V] => Boolean)(implicit F: Sync[F]): F[List[OptionalKV[K, V]]] =
    filter(f).typedDataset.take[F](params.showDs.rowNum).map(_.toList)

  def count(implicit F: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def show(implicit F: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

  def first(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]] = F.delay(dataset.rdd.cminOption)
  def last(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]]  = F.delay(dataset.rdd.cmaxOption)

  // state change
  def toProducerRecords: PrDataset[F, K, V] = {
    implicit val k: TypedEncoder[K] = codec.keyEncoder
    implicit val v: TypedEncoder[V] = codec.valEncoder
    new PrDataset(
      (typedDataset.deserialized.map(_.toNJProducerRecord)(codec.producerRecord)).dataset,
      cfg)
  }

  def save: RddFileHoarder[F, OptionalKV[K, V]] = {
    implicit val ss: SparkSession                  = dataset.sparkSession
    implicit val ac: NJAvroCodec[OptionalKV[K, V]] = codec.ateOptionalKV.avroCodec
    new RddFileHoarder[F, OptionalKV[K, V]](dataset.rdd)
  }

  private def bucketing(kv: OptionalKV[K, V]): Option[LocalDate] =
    Some(NJTimestamp(kv.timestamp).dayResolution(params.timeRange.zoneId))

  def partition: RddPartitionHoarder[F, OptionalKV[K, V], LocalDate] = {
    implicit val c: NJAvroCodec[OptionalKV[K, V]] = codec.ateOptionalKV.avroCodec
    implicit val ss: SparkSession                 = dataset.sparkSession
    new RddPartitionHoarder[F, OptionalKV[K, V], LocalDate](
      dataset.rdd,
      bucketing,
      params.datePartitionPathBuilder(params.topicName, _, _))
  }
}
