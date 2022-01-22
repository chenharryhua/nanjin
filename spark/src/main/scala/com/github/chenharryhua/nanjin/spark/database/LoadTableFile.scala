package com.github.chenharryhua.nanjin.spark.database

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.zaxxer.hikari.HikariConfig
import io.circe.Decoder as JsonDecoder
import kantan.csv.CsvConfiguration
import org.apache.spark.sql.{Dataset, SparkSession}

final class LoadTableFile[F[_], A] private[database] (
  td: TableDef[A],
  hikariConfig: HikariConfig,
  cfg: STConfig,
  ss: SparkSession) {
  private val ate: AvroTypedEncoder[A] = td.ate

  def parquet(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.parquet[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def avro(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.avro[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def circe(path: NJPath)(implicit ev: JsonDecoder[A], F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.circe[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def csv(path: NJPath, csvConfiguration: CsvConfiguration)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds = loaders.csv[A](path, ate, csvConfiguration, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def csv(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    csv(path, CsvConfiguration.rfc)

  def json(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.json[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def jackson(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.jackson[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }

  def binAvro(path: NJPath)(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking {
      val tds: Dataset[A] = loaders.binAvro[A](path, ate, ss)
      new TableDS[F, A](tds, td, hikariConfig, cfg)
    }
}
