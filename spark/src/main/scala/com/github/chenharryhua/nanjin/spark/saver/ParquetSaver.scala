package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractParquetSaver[F[_], A](
  rawAvroEncoder: Encoder[A],
  rawTypedEncoder: TypedEncoder[A])
    extends AbstractSaver[F, A] {

  private val sparkDatatype: DataType = SchemaConverters.toSqlType(rawAvroEncoder.schema).dataType
  private val schema: Schema          = SchemaConverters.toAvroType(sparkDatatype)

  private val sparkEncoder: TypedEncoder[A] = new TypedEncoder[A]()(rawTypedEncoder.classTag) {
    override def nullable: Boolean                          = rawTypedEncoder.nullable
    override def jvmRepr: DataType                          = rawTypedEncoder.jvmRepr
    override def catalystRepr: DataType                     = sparkDatatype
    override def fromCatalyst(path: Expression): Expression = rawTypedEncoder.fromCatalyst(path)
    override def toCatalyst(path: Expression): Expression   = rawTypedEncoder.toCatalyst(path)
  }

  private val avroEncoder: Encoder[A] =
    rawAvroEncoder.withSchema(SchemaFor(schema)).resolveEncoder()

  def withEncoder(enc: Encoder[A]): AbstractParquetSaver[F, A]
  def withSchema(schema: Schema): AbstractParquetSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession,
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    throw new Exception("not support single file")

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit =
    TypedDataset.createUnsafe(rdd.toDF(rawAvroEncoder, ss))(sparkEncoder).write.parquet(outPath)
}

final class ParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  outPath: String,
  val cfg: SaverConfig)
    extends AbstractParquetSaver[F, A](encoder, constraint) {

  override def repartition(num: Int): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd.repartition(num), encoder, constraint, outPath, cfg)

  override def withEncoder(enc: Encoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, outPath, cfg)

  override def withSchema(schema: Schema): ParquetSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new ParquetSaver[F, A](rdd, encoder.withSchema(schemaFor), constraint, outPath, cfg)
  }

  override def updateConfig(cfg: SaverConfig): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, outPath, cfg)

  override def errorIfExists: ParquetSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: ParquetSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ParquetSaver[F, A] = updateConfig(cfg.withIgnore)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.withHadoop.withMulti.evalConfig, blocker)

}

final class ParquetPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractParquetSaver[F, A](encoder, constraint) with Partition[F, A, K] {

  override def repartition(num: Int): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd.repartition(num),
      encoder,
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def withEncoder(enc: Encoder[A]): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, enc, constraint, bucketing, pathBuilder, cfg)

  override def withSchema(schema: Schema): ParquetPartitionSaver[F, A, K] = {
    val schemaFor = SchemaFor[A](schema)
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder.withSchema(schemaFor),
      constraint,
      bucketing,
      pathBuilder,
      cfg)
  }

  override def updateConfig(cfg: SaverConfig): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def errorIfExists: ParquetPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: ParquetPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ParquetPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def parallel(num: Long): ParquetPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): ParquetPartitionSaver[F, A, K1] =
    new ParquetPartitionSaver[F, A, K1](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(
      rdd,
      cfg.withHadoop.withMulti.withParallel(1).evalConfig,
      blocker,
      bucketing,
      pathBuilder)

}
