package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import org.apache.spark.sql.Dataset

final class DatabaseSave[F[_], A](ds: Dataset[A], ate: AvroTypedEncoder[A]) extends Serializable {

  def avro(path: String): SaveAvro[F, A] =
    AvroFileHoarder[F, A](ds.rdd, path, ate.avroCodec.avroEncoder).avro

  def binAvro(path: String): SaveBinaryAvro[F, A] =
    AvroFileHoarder[F, A](ds.rdd, path, ate.avroCodec.avroEncoder).binAvro

  def jackson(path: String): SaveJackson[F, A] =
    AvroFileHoarder[F, A](ds.rdd, path, ate.avroCodec.avroEncoder).jackson

  def circe(path: String): SaveCirce[F, A] =
    RddFileHoarder[F, A](ds.rdd, path).circe

  def text(path: String): SaveText[F, A] =
    RddFileHoarder[F, A](ds.rdd, path).text

  def protobuf(path: String): SaveProtobuf[F, A] =
    RddFileHoarder[F, A](ds.rdd, path).protobuf

  def objectFile(path: String): SaveObjectFile[F, A] =
    RddFileHoarder[F, A](ds.rdd, path).objectFile

  def csv(path: String): SaveCsv[F, A] =
    DatasetFileHoarder[F, A](ds, path).csv

  def json(path: String): SaveSparkJson[F, A] =
    DatasetFileHoarder[F, A](ds, path).json

  def parquet(path: String): SaveParquet[F, A] =
    DatasetFileHoarder[F, A](ds, path).parquet
}
