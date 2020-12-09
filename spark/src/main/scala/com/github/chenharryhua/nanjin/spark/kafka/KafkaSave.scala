package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import frameless.TypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class KafkaSave[F[_], A](rdd: RDD[A], codec: AvroCodec[A]) extends Serializable {

  def avro(path: String): SaveAvro[F, A] =
    AvroFileHoarder[F, A](rdd, path, codec.avroEncoder).avro

  def binAvro(path: String): SaveBinaryAvro[F, A] =
    AvroFileHoarder[F, A](rdd, path, codec.avroEncoder).binAvro

  def jackson(path: String): SaveJackson[F, A] =
    AvroFileHoarder[F, A](rdd, path, codec.avroEncoder).jackson

  def circe(path: String): SaveCirce[F, A] =
    RddFileHoarder[F, A](rdd, path).circe

  def text(path: String): SaveText[F, A] =
    RddFileHoarder[F, A](rdd, path).text

  def protobuf(path: String): SaveProtobuf[F, A] =
    RddFileHoarder[F, A](rdd, path).protobuf

  def objectFile(path: String): SaveObjectFile[F, A] =
    RddFileHoarder[F, A](rdd, path).objectFile

  def csv(path: String)(implicit te: TypedEncoder[A], ss: SparkSession): SaveCsv[F, A] = {
    val ate = AvroTypedEncoder(codec)
    DatasetFileHoarder[F, A](ate.normalize(rdd).dataset, path).csv
  }

  def json(path: String)(implicit te: TypedEncoder[A], ss: SparkSession): SaveSparkJson[F, A] = {
    val ate = AvroTypedEncoder(codec)
    DatasetFileHoarder[F, A](ate.normalize(rdd).dataset, path).json
  }

  def parquet(path: String)(implicit te: TypedEncoder[A], ss: SparkSession): SaveParquet[F, A] = {
    val ate = AvroTypedEncoder(codec)
    DatasetFileHoarder[F, A](ate.normalize(rdd).dataset, path).parquet
  }
}
