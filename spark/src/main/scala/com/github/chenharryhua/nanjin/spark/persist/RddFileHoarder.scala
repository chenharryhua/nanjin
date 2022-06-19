package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.NJFileFormat.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

sealed class RddFileHoarder[F[_], A](rdd: RDD[A]) extends Serializable {

// 1
  final def circe(path: NJPath)(implicit encoder: JsonEncoder[A]): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, HoarderConfig(path).outputFormat(Circe), isKeepNull = true, encoder)

// 2
  final def text(path: NJPath): SaveText[F, A] =
    new SaveText[F, A](rdd, HoarderConfig(path).outputFormat(Text), Text.suffix)

// 3
  final def objectFile(path: NJPath): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, HoarderConfig(path).outputFormat(JavaObject))

// 4
  final def protobuf(path: NJPath)(implicit evidence: A <:< GeneratedMessage): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, HoarderConfig(path).outputFormat(ProtoBuf), evidence)

// 5
  final def kantan(path: NJPath)(implicit encoder: HeaderEncoder[A]): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](rdd, CsvConfiguration.rfc, HoarderConfig(path).outputFormat(Kantan), encoder)

  final val asSource: RddStreamSource[F, A] = new RddStreamSource[F, A](rdd)
}

final class RddAvroFileHoarder[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A])
    extends RddFileHoarder[F, A](rdd) {

// 1
  def jackson(path: NJPath): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, HoarderConfig(path).outputFormat(Jackson))

// 2
  def avro(path: NJPath): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, HoarderConfig(path).outputFormat(Avro))

// 3
  def binAvro(path: NJPath): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, HoarderConfig(path).outputFormat(BinaryAvro))

// 4
  def parquet(path: NJPath): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, encoder, HoarderConfig(path).outputFormat(Parquet))
}
