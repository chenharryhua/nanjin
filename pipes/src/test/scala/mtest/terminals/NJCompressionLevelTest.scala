package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.*
import fs2.Stream
import io.circe.Json
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.*
import mtest.terminals.TestData.Tiger
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

/** Tests for compression level combinations that are not covered by other test files.
  *
  * Zstandard is only available for AvroCompression and ParquetCompression. For text-based formats (Text,
  * Circe, Jackson, Kantan, BinaryAvro), we test Deflate at various levels.
  */
class NJCompressionLevelTest extends AnyFunSuite {

  val fs2Root: Url = Url.parse("./data/test/terminals/compression-levels")

  // ---- Deflate level 9 on Text ----

  test("1.deflate level 9 on text - round trip") {
    val file = TextFile(_.Deflate(_.Nine))
    val tgt = fs2Root / "text-deflate9" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data = TestData.tigerSet
    val ts = Stream.emits(data.toList).covary[IO].map(_.asJson.noSpaces)
    val sink = hdp.sink(tgt).text
    val src = hdp.source(tgt).text(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    val result = action.unsafeRunSync().flatMap(io.circe.jawn.decode[Tiger](_).toOption).toSet
    assert(result == data)
  }

  // ---- Deflate level 1 on Circe ----

  test("2.deflate level 1 on circe - round trip") {
    val file = CirceFile(_.Deflate(_.One))
    val tgt = fs2Root / "circe-deflate1" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data = TestData.tigerSet
    val ts: Stream[IO, Json] = Stream.emits(data.toList).covary[IO].map(_.asJson)
    val sink = hdp.sink(tgt).circe
    val src = hdp.source(tgt).circe(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    val result = action.unsafeRunSync().flatMap(_.as[Tiger].toOption).toSet
    assert(result == data)
  }

  // ---- Deflate level 9 on BinaryAvro ----

  test("3.deflate level 9 on binary avro - round trip") {
    val file = BinAvroFile(_.Deflate(_.Nine))
    val tgt = fs2Root / "bin-avro-deflate9" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data: Set[GenericRecord] = pandaSet
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).binAvro
    val src = hdp.source(tgt).binAvro(100, pandaSchema)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  // ---- Deflate level 3 on Jackson ----

  test("4.deflate level 3 on jackson - round trip") {
    val file = JacksonFile(_.Deflate(_.Three))
    val tgt = fs2Root / "jackson-deflate3" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data: Set[GenericRecord] = pandaSet
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).jackson
    val src = hdp.source(tgt).jackson(100, pandaSchema)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  // ---- Deflate level 6 on Kantan CSV ----

  test("5.deflate level 6 on kantan csv - round trip") {
    val file = KantanFile(_.Deflate(_.Six))
    val tgt = fs2Root / "kantan-deflate6" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data = TestData.tigerSet
    val ts = Stream.emits(data.toList).covary[IO].map(t => List(t.id.toString, t.zooName.getOrElse("")))
    val sink = hdp.sink(tgt).kantan
    val src = hdp.source(tgt).kantan(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    val result = action.unsafeRunSync().flatMap {
      case a :: b :: Nil => scala.util.Try(a.toInt).toOption.map(Tiger(_, if (b.isEmpty) None else Some(b)))
      case _             => None
    }.toSet
    assert(result == data)
  }

  // ---- Zstandard on Avro (AvroCompression supports it) ----

  test("6.zstandard level 3 on avro - round trip") {
    val file = AvroFile(_.Zstandard(_.Three))
    val tgt = fs2Root / "avro-zstd3" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data: Set[GenericRecord] = pandaSet
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).avro(_.Zstandard(_.Three))
    val src = hdp.source(tgt).avro(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  // ---- Zstandard on Parquet (ParquetCompression supports it) ----

  test("7.zstandard level 5 on parquet - round trip") {
    val file = ParquetFile(_.Zstandard(_.Five))
    val tgt = fs2Root / "parquet-zstd5" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data: Set[GenericRecord] = pandaSet
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).parquet(_.withCompressionCodec(file.compression.codecName))
    val src = hdp.source(tgt).parquet(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  // ---- Xz on Avro (AvroCompression supports it) ----

  test("8.xz level 6 on avro - round trip") {
    val file = AvroFile(_.Xz(_.Six))
    val tgt = fs2Root / "avro-xz6" / file.fileName
    hdp.delete(tgt).unsafeRunSync()
    val data: Set[GenericRecord] = pandaSet
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).avro(_.Xz(_.Six))
    val src = hdp.source(tgt).avro(100)
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }
}
