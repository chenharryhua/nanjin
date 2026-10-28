package mtest.terminals

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.terminals.FileFormat
import com.github.chenharryhua.nanjin.terminals.FileFormat.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

class NJFileFormatTest extends FunSuite {

  private val CASES: List[(FileFormat, Int, String, String, String)] = List(
    (Unknown, -1, "unknown", "unknown", "unknown.unknown"),
    (Jackson, 1, "json", "jackson", "jackson.json"),
    (Circe, 2, "json", "circe", "circe.json"),
    (Text, 3, "txt", "plain", "plain.txt"),
    (Kantan, 4, "csv", "kantan", "kantan.csv"),
    (Parquet, 11, "parquet", "apache", "apache.parquet"),
    (Avro, 12, "avro", "data", "data.avro"),
    (BinaryAvro, 13, "avro", "binary", "binary.avro"),
    (JavaObject, 14, "obj", "java", "java.obj"),
    (Protobuf, 15, "pb", "google", "google.pb")
  )

  test("1.every FileFormat has its stable value, format, alias, and suffix") {
    assertEquals(CASES.map(_._1), FileFormat.values.toList)
    CASES.foreach { case (file_format, value, format, alias, suffix) =>
      assertEquals(file_format.value, value)
      assertEquals(file_format.format, format)
      assertEquals(file_format.alias, alias)
      assertEquals(file_format.suffix, suffix)
    }
  }

  test("2.numeric values and serialized suffixes are unique") {
    val values = CASES.map(_._2)
    val suffixes = CASES.map(_._5)
    assertEquals(values.distinct.size, values.size)
    assertEquals(suffixes.distinct.size, suffixes.size)
  }

  test("3.JSON encoding uses the exact suffix string") {
    CASES.foreach { case (file_format, _, _, _, suffix) =>
      assertEquals(file_format.asJson, Json.fromString(suffix))
    }
  }

  test("4.JSON decoding accepts every suffix and round-trips every case") {
    CASES.foreach { case (file_format, _, _, _, suffix) =>
      assertEquals(Json.fromString(suffix).as[FileFormat].toOption, Some(file_format))
      assertEquals(file_format.asJson.as[FileFormat].toOption, Some(file_format))
    }
  }

  test("5.JSON decoding rejects unknown and case-mismatched suffixes") {
    List("", "json", "Jackson.json", "unknown", "plain.csv").foreach { input =>
      val failure = Json.fromString(input).as[FileFormat].left.toOption
      assertEquals(failure.map(_.message), Some(s"invalid FileFormat: $input"))
    }
  }

  test("6.Show renders the serialized suffix") {
    CASES.foreach { case (file_format, _, _, _, suffix) =>
      assertEquals(file_format.show, suffix)
    }
  }
}
