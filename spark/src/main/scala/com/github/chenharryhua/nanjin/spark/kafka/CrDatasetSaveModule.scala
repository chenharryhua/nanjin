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

    def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(pathStr)

    def parquet(pathStr: String): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(pathStr)

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(pathStr)

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] =
      saver.circe(pathStr)

    def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def csv[A: RowEncoder](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: TypedEncoder[A]): CsvSaver[F, A] = {
      import ev.classTag
      saver.map(f).csv(pathStr)
    }

    def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      saver.map(f).protobuf(pathStr)

    def dump(pathStr: String): Dumper[F, OptionalKV[K, V]] =
      saver.dump(pathStr)
  }

  def save: FileSaver =
    new FileSaver(new NJRddFileSaver[F, OptionalKV[K, V]](crs.rdd))

}
