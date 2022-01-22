package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

final private[persist] class SaveModeAware[F[_]](
  saveMode: SaveMode,
  outPath: NJPath,
  hadoopConfiguration: Configuration)
    extends Serializable {

  def checkAndRun(f: F[Unit])(implicit F: Sync[F]): F[Unit] = {
    val hadoop: NJHadoop[F] = NJHadoop[F](hadoopConfiguration)

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

  def checkAndRun[A](f: Stream[F, A])(implicit F: Sync[F]): Stream[F, A] = {
    val hadoop: NJHadoop[F] = NJHadoop[F](hadoopConfiguration)

    saveMode match {
      case SaveMode.Append    => Stream.raiseError(new Exception("not support append operation"))
      case SaveMode.Overwrite => Stream.eval(hadoop.delete(outPath)) >> f
      case SaveMode.ErrorIfExists =>
        Stream.eval(hadoop.isExist(outPath)).flatMap {
          case true  => Stream.raiseError(new Exception(s"$outPath already exist"))
          case false => f
        }
      case SaveMode.Ignore =>
        Stream.eval(hadoop.isExist(outPath)).flatMap {
          case true  => Stream.empty
          case false => f
        }
    }
  }
}
