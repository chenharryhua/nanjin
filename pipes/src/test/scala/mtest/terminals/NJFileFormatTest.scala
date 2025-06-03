package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.FileFormat
import com.github.chenharryhua.nanjin.terminals.FileFormat.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class NJFileFormatTest extends AnyFunSuite {
  test("no dup") {
    val all: List[FileFormat] =
      List(Unknown, Jackson, Circe, Text, Kantan, Parquet, Avro, BinaryAvro, JavaObject, ProtoBuf)
    assert(all.map(_.suffix).distinct.size === all.size)
  }

  test("json") {
    val f1: FileFormat = FileFormat.Unknown
    val f2: FileFormat = FileFormat.Jackson
    val f3: FileFormat = FileFormat.Circe
    val f4: FileFormat = FileFormat.Text
    val f5: FileFormat = FileFormat.Kantan
    val f8: FileFormat = FileFormat.Parquet
    val f9: FileFormat = FileFormat.Avro
    val f10: FileFormat = FileFormat.BinaryAvro
    val f11: FileFormat = FileFormat.JavaObject
    val f12: FileFormat = FileFormat.ProtoBuf
    assert(f1.asJson.noSpaces === """ "unknown.unknown" """.trim)
    assert(f2.asJson.noSpaces === """ "jackson.json" """.trim)
    assert(f3.asJson.noSpaces === """ "circe.json" """.trim)
    assert(f4.asJson.noSpaces === """ "plain.txt" """.trim)
    assert(f5.asJson.noSpaces === """ "kantan.csv" """.trim)
    assert(f8.asJson.noSpaces === """ "apache.parquet" """.trim)
    assert(f9.asJson.noSpaces === """ "data.avro" """.trim)
    assert(f10.asJson.noSpaces === """ "binary.avro" """.trim)
    assert(f11.asJson.noSpaces === """ "java.obj" """.trim)
    assert(f12.asJson.noSpaces === """ "google.pb" """.trim)
  }
}
