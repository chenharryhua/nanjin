package mtest.common

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.common.NJFileFormat.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class NJFileFormatTest extends AnyFunSuite {
  test("no dup") {
    val all: List[NJFileFormat] =
      List(
        Unknown,
        Jackson,
        Circe,
        Text,
        Kantan,
        SparkJson,
        SparkCsv,
        Parquet,
        Avro,
        BinaryAvro,
        JavaObject,
        ProtoBuf)
    assert(all.map(_.suffix).distinct.size === all.size)
  }

  test("json") {
    import io.circe.syntax.*
    import io.circe.parser.decode
    val kantan: NJFileFormat = NJFileFormat.Kantan
    assert(decode[NJFileFormat](kantan.asJson.spaces2).toOption.get === kantan)
    assert(decode[NJFileFormat](NJFileFormat.Kantan.asJson.spaces2).toOption.get === kantan)

    assert(NJFileFormat.Unknown.asJson === Json.fromString("unknown.unknown"))
    assert(NJFileFormat.Circe.asJson === Json.fromString("circe.json"))
    assert(NJFileFormat.Text.asJson === Json.fromString("plain.txt"))
    assert(NJFileFormat.Kantan.asJson === Json.fromString("kantan.csv"))
    assert(NJFileFormat.SparkCsv.asJson === Json.fromString("spark.csv"))
    assert(NJFileFormat.SparkJson.asJson === Json.fromString("spark.json"))
    assert(NJFileFormat.Parquet.asJson === Json.fromString("apache.parquet"))
    assert(NJFileFormat.Avro.asJson === Json.fromString("data.avro"))
    assert(NJFileFormat.BinaryAvro.asJson === Json.fromString("binary.avro"))
    assert(NJFileFormat.JavaObject.asJson === Json.fromString("java.obj"))
    assert(NJFileFormat.ProtoBuf.asJson === Json.fromString("google.pb"))
  }
}
