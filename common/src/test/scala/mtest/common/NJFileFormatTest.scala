package mtest.common

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.common.NJFileFormat.*
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
}
