package com.github.chenharryhua.nanjin.sparkdb

sealed private[sparkdb] trait FileFormat {
  def defaultOptions: Map[String, String]
}

private[sparkdb] object FileFormat {

  case object Json extends FileFormat {
    val defaultOptions: Map[String, String] = Map.empty
  }

  case object Parquet extends FileFormat {
    val defaultOptions: Map[String, String] = Map.empty
  }

  case object Csv extends FileFormat {
    override def defaultOptions: Map[String, String] = Map("header" -> "true")
  }
}
