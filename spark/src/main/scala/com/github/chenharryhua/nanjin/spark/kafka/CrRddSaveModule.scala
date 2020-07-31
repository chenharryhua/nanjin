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

  final class FileSaver(saver: NJRddFileSaver[OptionalKV[K, V]]) {
    def avro(pathStr: String): AvroSaver[OptionalKV[K, V]]       = saver.avro(pathStr)
    def jackson(pathStr: String): JacksonSaver[OptionalKV[K, V]] = saver.jackson(pathStr)

    def parquet(pathStr: String)(implicit
      ev: TypedEncoder[OptionalKV[K, V]]): ParquetSaver[OptionalKV[K, V]] =
      saver.parquet(pathStr)

    def binAvro(pathStr: String): BinaryAvroSaver[OptionalKV[K, V]] = saver.binAvro(pathStr)

    def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[OptionalKV[K, V]] =
      saver.text(pathStr)

    def circe(pathStr: String)(implicit
      ev: JsonEncoder[OptionalKV[K, V]]): CirceJsonSaver[OptionalKV[K, V]] = saver.circe(pathStr)

    def javaObject(pathStr: String): JavaObjectSaver[OptionalKV[K, V]] =
      saver.javaObject(pathStr)

    def dump(pathStr: String): Dumper[OptionalKV[K, V]] =
      saver.dump(pathStr)
  }

  def save: FileSaver =
    new FileSaver(new NJRddFileSaver[OptionalKV[K, V]](rdd))

}
