package mtest.common

import com.github.chenharryhua.nanjin.common.*
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class NJCompressionTest extends AnyFunSuite {
  test("json") {
    val c1: CirceCompression      = NJCompression.Uncompressed
    val c2: NJCompression         = NJCompression.Snappy
    val c3: JacksonCompression    = NJCompression.Bzip2
    val c4: SparkJsonCompression  = NJCompression.Gzip
    val c5: TextCompression       = NJCompression.Lz4
    val c6: NJCompression         = NJCompression.Brotli
    val c7: NJCompression         = NJCompression.Lzo
    val c8: BinaryAvroCompression = NJCompression.Deflate(-1)
    val c9: AvroCompression       = NJCompression.Xz(2)
    val c10: ParquetCompression   = NJCompression.Zstandard(3)

    assert(decode[CirceCompression](c1.asJson.noSpaces).toOption.get === c1)
    assert(decode[NJCompression](c2.asJson.noSpaces).toOption.get === c2)
    assert(decode[JacksonCompression](c3.asJson.noSpaces).toOption.get === c3)
    assert(decode[SparkJsonCompression](c4.asJson.noSpaces).toOption.get === c4)
    assert(decode[TextCompression](c5.asJson.noSpaces).toOption.get === c5)
    assert(decode[NJCompression](c6.asJson.noSpaces).toOption.get === c6)
    assert(decode[NJCompression](c7.asJson.noSpaces).toOption.get === c7)
    assert(decode[BinaryAvroCompression](c8.asJson.noSpaces).toOption.get === c8)
    assert(decode[AvroCompression](c9.asJson.noSpaces).toOption.get === c9)
    assert(decode[ParquetCompression](c10.asJson.noSpaces).toOption.get === c10)
    assert(decode[NJCompression](""" "unknown" """).toOption.isEmpty)
    assert(decode[NJCompression](""" "snappy-2" """).toOption.isEmpty)
    assert(decode[NJCompression](""" "xz-a" """).toOption.isEmpty)

    assert(decode[CirceCompression](""" "lzo" """).toOption.isEmpty)
    assert(decode[CirceCompression](""" "gzip" """).toOption.nonEmpty)
  }
}
