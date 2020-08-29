package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.Sync
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedDataset
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}

final class TdsLoader[A](ate: AvroTypedEncoder[A], ss: SparkSession) extends Serializable {

  def avro(pathStr: String): TypedDataset[A] =
    ate.fromDF(ss.read.format("avro").load(pathStr))

  def parquet(pathStr: String): TypedDataset[A] =
    ate.fromDF(ss.read.parquet(pathStr))

  def csv(pathStr: String): TypedDataset[A] =
    ate.fromDF(ss.read.schema(ate.sparkStructType).csv(pathStr))

  def json(pathStr: String): TypedDataset[A] =
    ate.fromDF(ss.read.schema(ate.sparkStructType).json(pathStr))
}

final class TdsSaver[F[_], A](ds: Dataset[A], ate: AvroTypedEncoder[A]) extends Serializable {
  private val dfw: DataFrameWriter[A] = ate.fromDS(ds).write.mode(SaveMode.Overwrite)

  def avro(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(dfw.format("avro").save(pathStr))

  def parquet(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(dfw.parquet(pathStr))

  def json(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(dfw.json(pathStr))

  def csv(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(dfw.csv(pathStr))

  def text(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(dfw.text(pathStr))
}
