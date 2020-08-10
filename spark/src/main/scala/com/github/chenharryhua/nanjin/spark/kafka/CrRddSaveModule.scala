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
      saver.avro(params.outPath(NJFileFormat.Avro))

    def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(pathStr)

    def jackson: JacksonSaver[F, OptionalKV[K, V]] =
      saver.jackson(params.outPath(NJFileFormat.Jackson))

    def parquet(pathStr: String)(implicit
      ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def parquet(implicit ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(params.outPath(NJFileFormat.Parquet))

    def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(pathStr)

    def binAvro: BinaryAvroSaver[F, OptionalKV[K, V]] =
      saver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(pathStr)

    def text(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(params.outPath(NJFileFormat.Text))

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceSaver[F, OptionalKV[K, V]] =
      saver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[OptionalKV[K, V]]): CirceSaver[F, OptionalKV[K, V]] =
      saver.circe(params.outPath(NJFileFormat.Circe))

    def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def javaObject: JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(params.outPath(NJFileFormat.JavaObject))

    def csv[A: RowEncoder](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: TypedEncoder[A]): CsvSaver[F, A] = {
      import ev.classTag
      saver.map(f).csv(pathStr)
    }

    def csv[A: RowEncoder](f: OptionalKV[K, V] => A)(implicit ev: TypedEncoder[A]): CsvSaver[F, A] =
      csv(params.outPath(NJFileFormat.Csv))(f)

    def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      saver.map(f).protobuf(pathStr)

    def protobuf[A: ClassTag](f: OptionalKV[K, V] => A)(implicit
      ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
      protobuf(params.outPath(NJFileFormat.ProtoBuf))(f)

    def dump: Dumper[F, OptionalKV[K, V]] =
      saver.dump(params.replayPath)
  }

  def save: FileSaver =
    new FileSaver(new RddFileSaver[F, OptionalKV[K, V]](rdd))

}
