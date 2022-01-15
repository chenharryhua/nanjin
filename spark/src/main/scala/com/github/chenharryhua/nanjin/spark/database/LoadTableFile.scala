package com.github.chenharryhua.nanjin.spark.database

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.zaxxer.hikari.HikariConfig
import io.circe.Decoder as JsonDecoder
import kantan.csv.CsvConfiguration
import org.apache.spark.sql.SparkSession

final class LoadTableFile[F[_], A] private[database] (
  td: TableDef[A],
  hikariConfig: HikariConfig,
  cfg: STConfig,
  ss: SparkSession) {
  private val ate: AvroTypedEncoder[A] = td.ate

  def parquet(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.parquet[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def avro(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.avro[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def circe(pathStr: String)(implicit ev: JsonDecoder[A], F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.circe[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def csv(pathStr: String, csvConfiguration: CsvConfiguration)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.csv[A](pathStr, ate, csvConfiguration, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def csv(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    csv(pathStr, CsvConfiguration.rfc)

  def json(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.json[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def jackson(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.jackson[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def binAvro(pathStr: String)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.binAvro[A](pathStr, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }
}
