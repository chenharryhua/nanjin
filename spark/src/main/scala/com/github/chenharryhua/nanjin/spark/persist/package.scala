package com.github.chenharryhua.nanjin.spark

import cats.effect.Blocker
import org.apache.spark.sql.SparkSession

package object persist {

  def fileSink[F[_]](blocker: Blocker)(implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)

  def fileSource[F[_]](blocker: Blocker)(implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](blocker, ss.sparkContext.hadoopConfiguration)

}
