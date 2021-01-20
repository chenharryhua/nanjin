package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

final private[persist] class SaveModeAware[F[_]](
  saveMode: SaveMode,
  outPath: String,
  hadoopConfiguration: Configuration)
    extends Serializable {

  def checkAndRun(blocker: Blocker)(f: F[Unit])(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoop: NJHadoop[F] = NJHadoop[F](hadoopConfiguration, blocker)

    saveMode match {
      case SaveMode.Append    => f
      case SaveMode.Overwrite => hadoop.delete(outPath) >> f
      case SaveMode.ErrorIfExists =>
        hadoop.isExist(outPath).flatMap {
          case true  => F.raiseError(new Exception(s"$outPath already exist"))
          case false => f
        }
      case SaveMode.Ignore =>
        hadoop.isExist(outPath).flatMap {
          case true  => F.pure(())
          case false => f
        }
    }
  }
}
