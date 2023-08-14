package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.terminals.NJFileFormat.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.Stream
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

sealed class RddFileHoarder[F[_], A](frdd: F[RDD[A]]) extends Serializable {

// 1
  final def circe(path: NJPath)(implicit encoder: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](frdd, HoarderConfig(path).outputFormat(Circe), isKeepNull = true, encoder)

// 2
  final def text(path: NJPath): SaveText[F, A] =
    new SaveText[F, A](frdd, HoarderConfig(path).outputFormat(Text), Text.suffix)

// 3
  final def objectFile(path: NJPath): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](frdd, HoarderConfig(path).outputFormat(JavaObject))

// 4
  final def protobuf(path: NJPath)(implicit evidence: A <:< GeneratedMessage): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](frdd, HoarderConfig(path).outputFormat(ProtoBuf), evidence)

// 5
  final def kantan(path: NJPath, cfg: CsvConfiguration)(implicit
    encoder: RowEncoder[A]): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](frdd, cfg, HoarderConfig(path).outputFormat(Kantan), encoder)

  final def stream(chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, A] =
    Stream.eval(frdd).flatMap(rdd => Stream.fromBlockingIterator(rdd.toLocalIterator, chunkSize.value))

}

final class RddAvroFileHoarder[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A])
    extends RddFileHoarder[F, A](frdd) {

// 1
  def jackson(path: NJPath): SaveJackson[F, A] =
    new SaveJackson[F, A](frdd, encoder, HoarderConfig(path).outputFormat(Jackson))

// 2
  def avro(path: NJPath): SaveAvro[F, A] =
    new SaveAvro[F, A](frdd, encoder, HoarderConfig(path).outputFormat(Avro))

// 3
  def binAvro(path: NJPath): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](frdd, encoder, HoarderConfig(path).outputFormat(BinaryAvro))

// 4
  def parquet(path: NJPath): SaveParquet[F, A] =
    new SaveParquet[F, A](frdd, encoder, HoarderConfig(path).outputFormat(Parquet))
}
