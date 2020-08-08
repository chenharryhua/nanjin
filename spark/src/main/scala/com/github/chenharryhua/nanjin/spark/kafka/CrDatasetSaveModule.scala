package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.saver._
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

private[kafka] trait CrDatasetSaveModule[F[_], K, V] { self: CrDataset[F, K, V] =>

  final class FileSaver(saver: NJRddFileSaver[F, OptionalKV[K, V]]) {

    def avro(pathStr: String): AvroSaver[F, OptionalKV[K, V]] =
      saver.avro(pathStr)

    def avro: AvroSaver[F, OptionalKV[K, V]] = avro(params.outPath)

    def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(pathStr)

    def jackson: JacksonSaver[F, OptionalKV[K, V]] = jackson(params.outPath)

    def parquet(pathStr: String): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def parquet: ParquetSaver[F, OptionalKV[K, V]] = parquet(params.outPath)

    def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(pathStr)

    def binAvro: BinaryAvroSaver[F, OptionalKV[K, V]] = binAvro(params.outPath)

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(pathStr)

    def text(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      text(params.outPath)

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] =
      saver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] =
      circe(params.outPath)

    def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def javaObject: JavaObjectSaver[F, OptionalKV[K, V]] = javaObject(params.outPath)

    def csv[A: RowEncoder](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: TypedEncoder[A]): CsvSaver[F, A] = {
      import ev.classTag
      saver.map(f).csv(pathStr)
    }

    def csv[A: RowEncoder](f: OptionalKV[K, V] => A)(implicit ev: TypedEncoder[A]): CsvSaver[F, A] =
      csv(params.outPath)(f)

    def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      saver.map(f).protobuf(pathStr)

    def protobuf[A: ClassTag](f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      protobuf(params.outPath)(f)

    def dump: Dumper[F, OptionalKV[K, V]] =
      saver.dump(params.replayPath)
  }

  def save: FileSaver =
    new FileSaver(new NJRddFileSaver[F, OptionalKV[K, V]](crs.rdd))

}
