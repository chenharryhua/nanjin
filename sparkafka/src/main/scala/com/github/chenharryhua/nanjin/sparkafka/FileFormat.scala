package com.github.chenharryhua.nanjin.sparkafka

sealed private[sparkafka] trait FileFormat {
  def defaultOptions: Map[String, String]
}

private[sparkafka] object FileFormat {

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
