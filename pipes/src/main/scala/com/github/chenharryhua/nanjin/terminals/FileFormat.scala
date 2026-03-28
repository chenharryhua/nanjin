package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.syntax.eq.given
import io.circe.{Decoder, Encoder}

enum FileFormat(val value: Int, val format: String, val alias: String):
  case Unknown extends FileFormat(-1, "unknown", "unknown")
  // text
  case Jackson extends FileFormat(1, "json", "jackson")
  case Circe extends FileFormat(2, "json", "circe")
  case Text extends FileFormat(3, "txt", "plain")
  case Kantan extends FileFormat(4, "csv", "kantan")

  // binary
  case Parquet extends FileFormat(11, "parquet", "apache")
  case Avro extends FileFormat(12, "avro", "data")
  case BinaryAvro extends FileFormat(13, "avro", "binary")
  case JavaObject extends FileFormat(14, "obj", "java")
  case ProtoBuf extends FileFormat(15, "pb", "google")

  val suffix: String = s"$alias.$format"
end FileFormat

object FileFormat:
  given Encoder[FileFormat] = Encoder.encodeString.contramap(_.suffix)
  given Decoder[FileFormat] = Decoder.decodeString.emap { s =>
    FileFormat.values.find(_.suffix === s).toRight("invalid FileFormat: $s")
  }
  given Show[FileFormat] = _.suffix
end FileFormat
