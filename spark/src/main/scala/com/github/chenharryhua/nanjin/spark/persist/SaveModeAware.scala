package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.Hadoop
import io.lemonlabs.uri.Url
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

final private[persist] class SaveModeAware[F[_]](
  saveMode: SaveMode,
  outPath: Url,
  hadoopConfiguration: Configuration)
    extends Serializable {

  def checkAndRun(job: F[Unit])(implicit F: Sync[F]): F[Unit] = {
    val hadoop: Hadoop[F] = Hadoop[F](hadoopConfiguration)

    saveMode match {
      case SaveMode.Append        => job
      case SaveMode.Overwrite     => hadoop.delete(outPath) >> job
      case SaveMode.Ignore        => hadoop.isExist(outPath).ifM(F.unit, job)
      case SaveMode.ErrorIfExists =>
        hadoop.isExist(outPath).ifM(F.raiseError(new Exception(show"$outPath already exist")), job)
    }
  }
}
