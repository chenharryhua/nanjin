package mtest.common

import com.github.chenharryhua.nanjin.common.NJCompression
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import io.circe.parser.decode

class NJCompressionTest extends AnyFunSuite {
  test("json") {
    val c1: NJCompression  = NJCompression.Uncompressed
    val c2: NJCompression  = NJCompression.Snappy
    val c3: NJCompression  = NJCompression.Bzip2
    val c4: NJCompression  = NJCompression.Gzip
    val c5: NJCompression  = NJCompression.Lz4
    val c6: NJCompression  = NJCompression.Brotli
    val c7: NJCompression  = NJCompression.Lzo
    val c8: NJCompression  = NJCompression.Deflate(-1)
    val c9: NJCompression  = NJCompression.Xz(2)
    val c10: NJCompression = NJCompression.Zstandard(3)

    assert(decode[NJCompression](c1.asJson.noSpaces).toOption.get === c1)
    assert(decode[NJCompression](c2.asJson.noSpaces).toOption.get === c2)
    assert(decode[NJCompression](c3.asJson.noSpaces).toOption.get === c3)
    assert(decode[NJCompression](c4.asJson.noSpaces).toOption.get === c4)
    assert(decode[NJCompression](c5.asJson.noSpaces).toOption.get === c5)
    assert(decode[NJCompression](c6.asJson.noSpaces).toOption.get === c6)
    assert(decode[NJCompression](c7.asJson.noSpaces).toOption.get === c7)
    assert(decode[NJCompression](c8.asJson.noSpaces).toOption.get === c8)
    assert(decode[NJCompression](c9.asJson.noSpaces).toOption.get === c9)
    assert(decode[NJCompression](c10.asJson.noSpaces).toOption.get === c10)
    assert(decode[NJCompression]("unknown").toOption.isEmpty)
    assert(decode[NJCompression]("snappy-2").toOption.isEmpty)
    assert(decode[NJCompression]("xz-a").toOption.isEmpty)
  }
}
