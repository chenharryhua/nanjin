package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.effect.Blocker
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.saver._
import frameless.TypedEncoder
import frameless.cats.implicits.rddOps
import io.circe.{Encoder => JsonEncoder}

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  final class FileSaver(saver: NJRddFileSaver[F, OptionalKV[K, V]]) {
    def avro(pathStr: String): AvroSaver[F, OptionalKV[K, V]]       = saver.avro(pathStr)
    def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] = saver.jackson(pathStr)

    def parquet(pathStr: String)(implicit
      ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[F, OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] = saver.binAvro(pathStr)

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
      saver.text(pathStr)

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[F, OptionalKV[K, V]] = saver.circe(pathStr)

    def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def dump(pathStr: String): Dumper[F, OptionalKV[K, V]] =
      saver.dump(pathStr)
  }

  def save: FileSaver =
    new FileSaver(new NJRddFileSaver[F, OptionalKV[K, V]](rdd))

}
