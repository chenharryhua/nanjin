package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.saver._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import org.apache.spark.sql.Dataset

final class TableDataset[F[_], A](ds: Dataset[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit
  avroEncoder: AvroEncoder[A],
  typedEncoder: TypedEncoder[A])
    extends Serializable {

  val params: STParams = cfg.evalConfig

  def typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  def upload(implicit F: Sync[F]): F[Unit] =
    F.delay(
      sd.upload(ds, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, params.tableName))

  private val fileSaver: RddFileSaver[F, A] = new RddFileSaver[F, A](ds.rdd)

  object save extends Serializable {
    def avro(pathStr: String): AvroSaver[F, A] = fileSaver.avro(pathStr)
    def avro: AvroSaver[F, A]                  = fileSaver.avro(params.outPath(NJFileFormat.Avro))

    def binAvro(pathStr: String): BinaryAvroSaver[F, A] = fileSaver.binAvro(pathStr)
    def binAvro: BinaryAvroSaver[F, A]                  = fileSaver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

    def circe(pathStr: String)(implicit ev: JsonEncoder[A]): CirceSaver[F, A] =
      fileSaver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[A]): CirceSaver[F, A] =
      fileSaver.circe(params.outPath(NJFileFormat.Circe))

    def jackson(pathStr: String): JacksonSaver[F, A] = fileSaver.jackson(pathStr)
    def jackson: JacksonSaver[F, A]                  = fileSaver.jackson(params.outPath(NJFileFormat.Jackson))

    def parquet(pathStr: String): ParquetSaver[F, A] = fileSaver.parquet(pathStr)
    def parquet: ParquetSaver[F, A]                  = fileSaver.parquet(params.outPath(NJFileFormat.Parquet))

    def csv(pathStr: String)(implicit ev: RowEncoder[A]): CsvSaver[F, A] = fileSaver.csv(pathStr)

    def csv(implicit ev: RowEncoder[A]): CsvSaver[F, A] =
      fileSaver.csv(params.outPath(NJFileFormat.Csv))

    def javaObject(pathStr: String): JavaObjectSaver[F, A] = fileSaver.javaObject(pathStr)

    def javaObject: JavaObjectSaver[F, A] =
      fileSaver.javaObject(params.outPath(NJFileFormat.JavaObject))

    object partition extends Serializable {

      def avro: AvroPartitionSaver[F, A, Unit] =
        fileSaver.partition.avro(a => (), Unit => params.outPath(NJFileFormat.Avro))

      def jackson: JacksonPartitionSaver[F, A, Unit] =
        fileSaver.partition.jackson(a => (), Unit => params.outPath(NJFileFormat.Jackson))

      def circe(implicit ev: JsonEncoder[A]): CircePartitionSaver[F, A, Unit] =
        fileSaver.partition.circe(a => (), Unit => params.outPath(NJFileFormat.Circe))

      def parquet: ParquetPartitionSaver[F, A, Unit] =
        fileSaver.partition.parquet(a => (), Unit => params.outPath(NJFileFormat.Parquet))
    }
  }
}
