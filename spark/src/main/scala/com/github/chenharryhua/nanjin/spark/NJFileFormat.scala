package com.github.chenharryhua.nanjin.spark

private[spark] trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}

sealed abstract class NJFileFormat {
  def format: String
}

object NJFileFormat {

  case object Json extends NJFileFormat {
    val format = "json"
  }

  case object Parquet extends NJFileFormat {
    val format = "parquet"
  }

  case object Avro extends NJFileFormat {
    val format = "avro"
  }
}
