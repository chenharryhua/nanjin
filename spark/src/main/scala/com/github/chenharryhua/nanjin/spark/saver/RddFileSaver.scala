package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class RddFileSaver[F[_], A](cfg: SaverConfig) extends Serializable {

  private def updateConfig(cfg: SaverConfig): RddFileSaver[F, A] =
    new RddFileSaver[F, A](cfg)

  def overwrite: RddFileSaver[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: RddFileSaver[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: RddFileSaver[F, A] = updateConfig(cfg.withIgnore)

  object single {}
}
