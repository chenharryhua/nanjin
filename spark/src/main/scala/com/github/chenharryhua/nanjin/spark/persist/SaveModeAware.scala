package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.fileSink
import org.apache.spark.sql.{SaveMode, SparkSession}

final class SaveModeAware[F[_]](saveMode: SaveMode, outPath: String, sparkSession: SparkSession)
    extends Serializable {
  implicit val ss: SparkSession = sparkSession

  def checkAndRun(blocker: Blocker)(f: F[Unit])(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    saveMode match {
      case SaveMode.Append    => F.raiseError(new Exception("append mode is not support"))
      case SaveMode.Overwrite => fileSink[F](blocker).delete(outPath) >> f
      case SaveMode.ErrorIfExists =>
        fileSink[F](blocker).isExist(outPath).flatMap {
          case true  => F.raiseError(new Exception(s"$outPath already exist"))
          case false => f
        }
      case SaveMode.Ignore =>
        fileSink[F](blocker).isExist(outPath).flatMap {
          case true  => F.pure(())
          case false => f
        }
    }
}
