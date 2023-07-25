package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.NJFileFormat
import com.github.chenharryhua.nanjin.terminals.NJFileFormat.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class NJFileFormatTest extends AnyFunSuite {
  test("no dup") {
    val all: List[NJFileFormat] =
      List(Unknown, Jackson, Circe, Text, Kantan, Parquet, Avro, BinaryAvro, JavaObject, ProtoBuf)
    assert(all.map(_.suffix).distinct.size === all.size)
  }

  test("json") {
    val f1: NJFileFormat  = NJFileFormat.Unknown
    val f2: NJFileFormat  = NJFileFormat.Jackson
    val f3: NJFileFormat  = NJFileFormat.Circe
    val f4: NJFileFormat  = NJFileFormat.Text
    val f5: NJFileFormat  = NJFileFormat.Kantan
    val f8: NJFileFormat  = NJFileFormat.Parquet
    val f9: NJFileFormat  = NJFileFormat.Avro
    val f10: NJFileFormat = NJFileFormat.BinaryAvro
    val f11: NJFileFormat = NJFileFormat.JavaObject
    val f12: NJFileFormat = NJFileFormat.ProtoBuf
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
