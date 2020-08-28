package com.github.chenharryhua.nanjin.spark.database

import cats.Show
import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.saver._
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import org.apache.spark.sql.Dataset

final class TableDataset[F[_], A](ds: Dataset[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit avroTypedEncoder: AvroTypedEncoder[A])
    extends Serializable {

  import avroTypedEncoder.sparkEncoder.classTag

  implicit val avroEncoder: AvroEncoder[A] = avroTypedEncoder.sparkAvroEncoder
  implicit val avorDecoder: AvroDecoder[A] = avroTypedEncoder.sparkAvroDecoder

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](ds.repartition(num), dbSettings, cfg)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String) =
    new TableDataset[F, A](ds, dbSettings, cfg.withPathBuilder(f))

  def upload: DbUploader[F, A] = new DbUploader[F, A](ds, dbSettings, avroEncoder, cfg)

  private val saver: RddFileSaver[F, A] = new RddFileSaver[F, A](ds.rdd)

  object save extends Serializable {

    def avro(pathStr: String): ConfigFileSaver[F, A] =
      saver.avro(pathStr)

    def avro: ConfigFileSaver[F, A] =
      saver.avro(params.outPath(NJFileFormat.Avro))

    def binAvro(pathStr: String): ConfigFileSaver[F, A] =
      saver.binAvro(pathStr)

    def binAvro: ConfigFileSaver[F, A] =
      saver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

    def circe(pathStr: String)(implicit ev: JsonEncoder[A]): ConfigFileSaver[F, A] =
      saver.circe(pathStr)

    def circe(implicit ev: JsonEncoder[A]): ConfigFileSaver[F, A] =
      saver.circe(params.outPath(NJFileFormat.Circe))

    def jackson(pathStr: String): ConfigFileSaver[F, A] =
      saver.jackson(pathStr)

    def jackson: ConfigFileSaver[F, A] =
      saver.jackson(params.outPath(NJFileFormat.Jackson))

    def parquet(pathStr: String): ConfigFileSaver[F, A] =
      saver.parquet(pathStr)

    def parquet: ConfigFileSaver[F, A] =
      saver.parquet(params.outPath(NJFileFormat.Parquet))

    def csv(pathStr: String)(implicit ev: RowEncoder[A]): ConfigFileSaver[F, A] =
      saver.csv(pathStr)

    def csv(implicit ev: RowEncoder[A]): ConfigFileSaver[F, A] =
      saver.csv(params.outPath(NJFileFormat.Csv))

    def javaObject(pathStr: String): ConfigFileSaver[F, A] =
      saver.javaObject(pathStr)

    def javaObject: ConfigFileSaver[F, A] =
      saver.javaObject(params.outPath(NJFileFormat.JavaObject))

    def text(pathStr: String)(implicit ev: Show[A]): ConfigFileSaver[F, A] =
      saver.text(pathStr)

    def text(implicit ev: Show[A]): ConfigFileSaver[F, A] =
      saver.text(params.outPath(NJFileFormat.Text))

  }
}
