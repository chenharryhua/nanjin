package com.github.chenharryhua.nanjin.spark.persist

import cats.{Endo, Show}
import com.github.chenharryhua.nanjin.terminals.NJFileFormat.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

sealed class RddFileHoarder[A](rdd: RDD[A]) extends Serializable {

// 1
  final def circe(path: NJPath)(implicit encoder: JsonEncoder[A]): SaveCirce[A] =
    new SaveCirce[A](rdd, HoarderConfig(path).outputFormat(Circe), isKeepNull = true, encoder)

// 2
  final def text(path: NJPath)(implicit encoder: Show[A]): SaveText[A] =
    new SaveText[A](rdd, HoarderConfig(path).outputFormat(Text), encoder, Text.suffix)

// 3
  final def objectFile(path: NJPath): SaveObjectFile[A] =
    new SaveObjectFile[A](rdd, HoarderConfig(path).outputFormat(JavaObject))

// 4
  final def protobuf(path: NJPath)(implicit evidence: A <:< GeneratedMessage): SaveProtobuf[A] =
    new SaveProtobuf[A](rdd, HoarderConfig(path).outputFormat(ProtoBuf), evidence)

// 5
  final def kantan(path: NJPath, cfg: CsvConfiguration)(implicit encoder: RowEncoder[A]): SaveKantanCsv[A] =
    new SaveKantanCsv[A](rdd, cfg, HoarderConfig(path).outputFormat(Kantan), encoder)

  final def kantan(path: NJPath, f: Endo[CsvConfiguration])(implicit
    encoder: RowEncoder[A]): SaveKantanCsv[A] =
    kantan(path, f(CsvConfiguration.rfc))

  final def kantan(path: NJPath)(implicit encoder: RowEncoder[A]): SaveKantanCsv[A] =
    kantan(path, CsvConfiguration.rfc)
}

final class RddAvroFileHoarder[A](rdd: RDD[A], encoder: AvroEncoder[A]) extends RddFileHoarder[A](rdd) {

// 1
  def jackson(path: NJPath): SaveJackson[A] =
    new SaveJackson[A](rdd, encoder, HoarderConfig(path).outputFormat(Jackson))

// 2
  def avro(path: NJPath): SaveAvro[A] =
    new SaveAvro[A](rdd, encoder, HoarderConfig(path).outputFormat(Avro))

// 3
  def binAvro(path: NJPath): SaveBinaryAvro[A] =
    new SaveBinaryAvro[A](rdd, encoder, HoarderConfig(path).outputFormat(BinaryAvro))

// 4
  def parquet(path: NJPath): SaveParquet[A] =
    new SaveParquet[A](rdd, encoder, HoarderConfig(path).outputFormat(Parquet))
}
