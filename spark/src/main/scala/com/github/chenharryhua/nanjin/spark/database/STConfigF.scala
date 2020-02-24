package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.NJRepartition
import org.apache.spark.sql.SaveMode

trait STConfigF[A]

object STConfigF {
  final case class DefaultParams[K]() extends STConfigF[K]

  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]
  final case class WithFileSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithRepartition[K](value: NJRepartition, cont: K) extends STConfigF[K]

  final case class WithShowRows[K](value: Int, cont: K) extends STConfigF[K]
  final case class WithShowTruncate[K](isTruncate: Boolean, cont: K) extends STConfigF[K]

  final case class WithPathBuilder[K](value: Reader[TablePathBuild, String], cont: K)
      extends STConfigF[K]

}
