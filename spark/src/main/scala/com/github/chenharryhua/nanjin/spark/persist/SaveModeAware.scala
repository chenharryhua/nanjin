package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import cats.syntax.all._
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

final private[persist] class SaveModeAware[F[_]](
  saveMode: SaveMode,
  outPath: String,
  hadoopConfiguration: Configuration)
    extends Serializable {

  def checkAndRun(action: F[Unit])(implicit F: Sync[F]): F[Unit] = {
    val hadoop: NJHadoop[F] = NJHadoop[F](hadoopConfiguration)

    saveMode match {
      case SaveMode.Append    => action
      case SaveMode.Overwrite => hadoop.delete(outPath) >> action
      case SaveMode.ErrorIfExists =>
        hadoop.isExist(outPath).flatMap {
          case true  => F.raiseError(new Exception(s"$outPath already exist"))
          case false => action
        }
      case SaveMode.Ignore =>
        hadoop.isExist(outPath).flatMap {
          case true  => F.pure(())
          case false => action
        }
    }
  }
}
