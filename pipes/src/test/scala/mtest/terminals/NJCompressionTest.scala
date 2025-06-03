package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.*
import io.circe.jawn.decode
import org.scalatest.funsuite.AnyFunSuite
import io.circe.syntax.*
import eu.timepit.refined.auto.*

class NJCompressionTest extends AnyFunSuite {
  test("json") {
    val c1: CirceCompression = Compression.Uncompressed
    val c2: Compression = Compression.Snappy
    val c3: JacksonCompression = Compression.Bzip2
    val c5: TextCompression = Compression.Lz4
    val c6: Compression = Compression.Brotli
    val c7: Compression = Compression.Lzo
    val c8: BinaryAvroCompression = Compression.Deflate(1)
    val c9: AvroCompression = Compression.Xz(2)
    val c10: ParquetCompression = Compression.Zstandard(3)

    assert(decode[CirceCompression](c1.asJson.noSpaces).toOption.get === c1)
    assert(decode[Compression](c2.asJson.noSpaces).toOption.get === c2)
    assert(decode[JacksonCompression](c3.asJson.noSpaces).toOption.get === c3)
    assert(decode[TextCompression](c5.asJson.noSpaces).toOption.get === c5)
    assert(decode[Compression](c6.asJson.noSpaces).toOption.get === c6)
    assert(decode[Compression](c7.asJson.noSpaces).toOption.get === c7)
    assert(decode[BinaryAvroCompression](c8.asJson.noSpaces).toOption.get === c8)
    assert(decode[AvroCompression](c9.asJson.noSpaces).toOption.get === c9)
    assert(decode[ParquetCompression](c10.asJson.noSpaces).toOption.get === c10)
    assert(decode[Compression](""" "unknown" """).toOption.isEmpty)
    assert(decode[Compression](""" "snappy-2" """).toOption.isEmpty)
    assert(decode[Compression](""" "xz-a" """).toOption.isEmpty)

    assert(decode[CirceCompression](""" "lzo" """).toOption.isEmpty)
    assert(decode[CirceCompression](""" "gzip" """).toOption.nonEmpty)
    assert(c10.productPrefix === "Zstandard")
    assert(c7.productPrefix == "Lzo")
  }
  test("filename") {
    assert(Compression.Snappy.fileName(FileFormat.Avro) === "snappy.data.avro")
    assert(Compression.Snappy.fileName(FileFormat.Circe) === "circe.json.snappy")
  }
}
