package com.github.chenharryhua.nanjin.spark.database

import cats.Show
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

  def map[B: AvroEncoder: TypedEncoder](f: A => B): TableDataset[F, B] =
    new TableDataset[F, B](typedDataset.deserialized.map(f).dataset, dbSettings, cfg)

  def flatMap[B: AvroEncoder: TypedEncoder](f: A => TraversableOnce[B]): TableDataset[F, B] =
    new TableDataset[F, B](typedDataset.deserialized.flatMap(f).dataset, dbSettings, cfg)

  def upload(implicit F: Sync[F]): F[Unit] =
    F.delay(
      sd.upload(ds, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, params.tableName))

  private val saver: RddFileSaver[F, A] = new RddFileSaver[F, A](ds.rdd)

  object save extends Serializable {

    def avro(pathStr: String): AvroSaver[F, A] =
      saver.avro(pathStr)

    def avro: AvroSaver[F, A] =
      saver.avro(params.outPath(NJFileFormat.Avro))

    def binAvro(pathStr: String): BinaryAvroSaver[F, A] =
      saver.binAvro(pathStr)

    def binAvro: BinaryAvroSaver[F, A] =
      saver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

    def circe(pathStr: String)(implicit ev: JsonEncoder[A]): CirceSaver[F, A] =
      saver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[A]): CirceSaver[F, A] =
      saver.circe(params.outPath(NJFileFormat.Circe))

    def jackson(pathStr: String): JacksonSaver[F, A] =
      saver.jackson(pathStr)

    def jackson: JacksonSaver[F, A] =
      saver.jackson(params.outPath(NJFileFormat.Jackson))

    def parquet(pathStr: String): ParquetSaver[F, A] =
      saver.parquet(pathStr)

    def parquet: ParquetSaver[F, A] =
      saver.parquet(params.outPath(NJFileFormat.Parquet))

    def csv(pathStr: String)(implicit ev: RowEncoder[A]): CsvSaver[F, A] =
      saver.csv(pathStr)

    def csv(implicit ev: RowEncoder[A]): CsvSaver[F, A] =
      saver.csv(params.outPath(NJFileFormat.Csv))

    def javaObject(pathStr: String): JavaObjectSaver[F, A] =
      saver.javaObject(pathStr)

    def javaObject: JavaObjectSaver[F, A] =
      saver.javaObject(params.outPath(NJFileFormat.JavaObject))

    def text(pathStr: String)(implicit ev: Show[A]): TextSaver[F, A] =
      saver.text(pathStr)

    def text(implicit ev: Show[A]): TextSaver[F, A] =
      saver.text(params.outPath(NJFileFormat.Text))

    object partition extends Serializable {

      def avro: AvroPartitionSaver[F, A, Unit] =
        saver.partition.avro(a => (), Unit => params.outPath(NJFileFormat.Avro))

      def jackson: JacksonPartitionSaver[F, A, Unit] =
        saver.partition.jackson(a => (), Unit => params.outPath(NJFileFormat.Jackson))

      def circe(implicit ev: JsonEncoder[A]): CircePartitionSaver[F, A, Unit] =
        saver.partition.circe(a => (), Unit => params.outPath(NJFileFormat.Circe))

      def parquet: ParquetPartitionSaver[F, A, Unit] =
        saver.partition.parquet(a => (), Unit => params.outPath(NJFileFormat.Parquet))

      def csv(implicit ev: RowEncoder[A]): CsvPartitionSaver[F, A, Unit] =
        saver.partition.csv(a => (), Unit => params.outPath(NJFileFormat.Csv))

      def text(implicit ev: Show[A]): TextPartitionSaver[F, A, Unit] =
        saver.partition.text(a => (), Unit => params.outPath(NJFileFormat.Text))
    }
  }
}
