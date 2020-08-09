package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.saver._
import frameless.TypedEncoder
import frameless.cats.implicits.rddOps
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  final class FileSaver(saver: RddFileSaver[F, OptionalKV[K, V]]) {

    def avro(pathStr: String): AvroSaver[F, OptionalKV[K, V]] =
      saver.avro(pathStr)

    def avro: AvroSaver[F, OptionalKV[K, V]] =
      saver.avro(cfg.withFileFormat(NJFileFormat.Avro).evalConfig.outPath)

    def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(pathStr)

    def jackson: JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(cfg.withFileFormat(NJFileFormat.Jackson).evalConfig.outPath)

    def parquet(pathStr: String)(implicit
      ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def parquet(implicit ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(cfg.withFileFormat(NJFileFormat.Parquet).evalConfig.outPath)

    def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(pathStr)

    def binAvro: BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(cfg.withFileFormat(NJFileFormat.BinaryAvro).evalConfig.outPath)

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(pathStr)

    def text(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(cfg.withFileFormat(NJFileFormat.Text).evalConfig.outPath)

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] =
      saver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] =
      saver.circe(cfg.withFileFormat(NJFileFormat.Circe).evalConfig.outPath)

    def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def javaObject: JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(cfg.withFileFormat(NJFileFormat.JavaObject).evalConfig.outPath)

    def csv[A: RowEncoder](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: TypedEncoder[A]): CsvSaver[F, A] = {
      import ev.classTag
      saver.map(f).csv(pathStr)
    }

    def csv[A: RowEncoder](f: OptionalKV[K, V] => A)(implicit ev: TypedEncoder[A]): CsvSaver[F, A] =
      csv(cfg.withFileFormat(NJFileFormat.Csv).evalConfig.outPath)(f)

    def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      saver.map(f).protobuf(pathStr)

    def protobuf[A: ClassTag](f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      protobuf(cfg.withFileFormat(NJFileFormat.ProtoBuf).evalConfig.outPath)(f)

    def dump: Dumper[F, OptionalKV[K, V]] =
      saver.dump(params.replayPath)
  }

  def save: FileSaver =
    new FileSaver(new RddFileSaver[F, OptionalKV[K, V]](rdd))

}
