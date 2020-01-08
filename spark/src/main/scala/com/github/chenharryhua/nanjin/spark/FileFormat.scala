package com.github.chenharryhua.nanjin.spark

private[spark] trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}

sealed abstract class FileFormat {
  def format: String
}

object FileFormat {

  case object Json extends FileFormat {
    val format = "json"
  }

  case object Parquet extends FileFormat {
    val format = "parquet"
  }

  case object Avro extends FileFormat {
    val format = "avro"
  }
}
