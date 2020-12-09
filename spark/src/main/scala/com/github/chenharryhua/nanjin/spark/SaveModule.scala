package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.persist.{
  SaveAvro,
  SaveBinaryAvro,
  SaveCirce,
  SaveCsv,
  SaveJackson,
  SaveObjectFile,
  SaveParquet,
  SaveProtobuf,
  SaveSparkJson,
  SaveText,
  AvroFileHoarder => AH,
  DatasetFileHoarder => DH,
  RddFileHoarder => RH
}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

sealed class SaveRdd[F[_], A](rdd: RDD[A]) extends Serializable {
  final def circe(path: String): SaveCirce[F, A]           = RH[F, A](rdd, path).circe
  final def text(path: String): SaveText[F, A]             = RH[F, A](rdd, path).text
  final def protobuf(path: String): SaveProtobuf[F, A]     = RH[F, A](rdd, path).protobuf
  final def objectFile(path: String): SaveObjectFile[F, A] = RH[F, A](rdd, path).objectFile
}

sealed class SaveAvroRdd[F[_], A](rdd: RDD[A], encoder: Encoder[A]) extends SaveRdd[F, A](rdd) {
  final def avro(path: String): SaveAvro[F, A]          = AH[F, A](rdd, path, encoder).avro
  final def binAvro(path: String): SaveBinaryAvro[F, A] = AH[F, A](rdd, path, encoder).binAvro
  final def jackson(path: String): SaveJackson[F, A]    = AH[F, A](rdd, path, encoder).jackson
}

sealed class SaveDataset[F[_], A](ds: Dataset[A]) extends SaveRdd[F, A](ds.rdd) {
  final def csv(path: String): SaveCsv[F, A]         = DH[F, A](ds, path).csv
  final def json(path: String): SaveSparkJson[F, A]  = DH[F, A](ds, path).json
  final def parquet(path: String): SaveParquet[F, A] = DH[F, A](ds, path).parquet
}

final class SaveAvroDataset[F[_], A](ds: Dataset[A], encoder: Encoder[A])
    extends SaveDataset[F, A](ds) {
  def avro(path: String): SaveAvro[F, A]          = AH[F, A](ds.rdd, path, encoder).avro
  def binAvro(path: String): SaveBinaryAvro[F, A] = AH[F, A](ds.rdd, path, encoder).binAvro
  def jackson(path: String): SaveJackson[F, A]    = AH[F, A](ds.rdd, path, encoder).jackson
}
