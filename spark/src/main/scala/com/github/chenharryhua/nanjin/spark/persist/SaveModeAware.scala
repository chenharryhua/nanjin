package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import org.apache.spark.sql.{SaveMode, SparkSession}

final private[persist] class SaveModeAware[F[_]](
  saveMode: SaveMode,
  outPath: String,
  sparkSession: SparkSession)
    extends Serializable {
  implicit val ss: SparkSession = sparkSession

  def checkAndRun(blocker: Blocker)(
    f: F[Unit])(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoop = new NJHadoop[F](sparkSession.sparkContext.hadoopConfiguration)

    saveMode match {
      case SaveMode.Append    => F.raiseError(new Exception("append mode is not support"))
      case SaveMode.Overwrite => hadoop.delete(outPath, blocker) >> f
      case SaveMode.ErrorIfExists =>
        hadoop.isExist(outPath, blocker).flatMap {
          case true  => F.raiseError(new Exception(s"$outPath already exist"))
          case false => f
        }
      case SaveMode.Ignore =>
        hadoop.isExist(outPath, blocker).flatMap {
          case true  => F.pure(())
          case false => f
        }
    }
  }
}
