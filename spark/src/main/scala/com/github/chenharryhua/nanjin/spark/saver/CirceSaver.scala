package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import org.apache.avro.Schema

sealed abstract private[saver] class AbstractCirceSaver[F[_], A](
  encoder: Encoder[A],
  avroEncoder: AvroEncoder[A],
  avroDecoder: AvroDecoder[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: Encoder[A] = encoder

  def single: AbstractCirceSaver[F, A]
  def multi: AbstractCirceSaver[F, A]

  def withEncoder(avroEncoder: AvroEncoder[A]): AbstractCirceSaver[F, A]
  def withSchema(schema: Schema): AbstractCirceSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession,
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd
      .map(a => encoder(avroDecoder.decode(avroEncoder.encode(a))))
      .stream[F]
      .through(fileSink[F](blocker)(ss).circe(outPath))
      .compile
      .drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit =
    rdd
      .map(a => encoder(avroDecoder.decode(avroEncoder.encode(a))).noSpaces)
      .saveAsTextFile(outPath)

  final override protected def toDataFrame(rdd: RDD[A], ss: SparkSession): DataFrame =
    rdd.toDF(avroEncoder, ss)
}

final class CirceSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  avroEncoder: AvroEncoder[A],
  avroDecoder: AvroDecoder[A],
  outPath: String,
  cfg: SaverConfig)
    extends AbstractCirceSaver[F, A](encoder, avroEncoder, avroDecoder) {

  override def repartition(num: Int): CirceSaver[F, A] =
    new CirceSaver(rdd.repartition(num), encoder, avroEncoder, avroDecoder, outPath, cfg)

  override def withEncoder(avroEncoder: AvroEncoder[A]): CirceSaver[F, A] =
    new CirceSaver(rdd, encoder, avroEncoder, avroDecoder, outPath, cfg)

  override def withSchema(schema: Schema): CirceSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new CirceSaver[F, A](rdd, encoder, avroEncoder.withSchema(schemaFor), avroDecoder, outPath, cfg)
  }

  override def updateConfig(cfg: SaverConfig): CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, encoder, avroEncoder, avroDecoder, outPath, cfg)

  override def errorIfExists: CirceSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: CirceSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: CirceSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: CirceSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: CirceSaver[F, A]  = updateConfig(cfg.withMulti)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class CircePartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  avroEncoder: AvroEncoder[A],
  avroDecoder: AvroDecoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractCirceSaver[F, A](encoder, avroEncoder, avroDecoder) with Partition[F, A, K] {

  override def repartition(num: Int): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](
      rdd.repartition(num),
      encoder,
      avroEncoder,
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)

  override def withEncoder(avroEncoder: AvroEncoder[A]): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](
      rdd,
      encoder,
      avroEncoder,
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)

  override def withSchema(schema: Schema): CircePartitionSaver[F, A, K] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new CircePartitionSaver[F, A, K](
      rdd,
      encoder,
      avroEncoder.withSchema(schemaFor),
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)
  }

  override def updateConfig(cfg: SaverConfig): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](
      rdd,
      encoder,
      avroEncoder,
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)

  override def errorIfExists: CircePartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: CircePartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: CircePartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: CircePartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: CircePartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def parallel(num: Long): CircePartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): CircePartitionSaver[F, A, K1] =
    new CircePartitionSaver[F, A, K1](
      rdd,
      encoder,
      avroEncoder,
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)

  override def rePath(pathBuilder: K => String): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](
      rdd,
      encoder,
      avroEncoder,
      avroDecoder,
      bucketing,
      pathBuilder,
      cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)

}
