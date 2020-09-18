package mtest.spark

import better.files._
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.spark.persist.{
  fileSink,
  fileSource,
  SingleFileSink,
  SingleFileSource
}
import frameless.TypedEncoder
import frameless.cats.implicits._
import fs2.Stream
import io.circe.generic.auto._
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import kantan.csv.generic._
import mtest.spark.pb.test.Whale
import org.apache.avro.file.CodecFactory
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object SingleFileTestData {
  final case class Swordfish(from: String, weight: Float, code: Int)

  val fishes =
    List(
      Swordfish("pacific|occean", 10.3f, Random.nextInt()),
      Swordfish("india occean", 2.5f, Random.nextInt()),
      Swordfish("atlantic occean", 5.5f, Random.nextInt())
    )

  val fishStream: Stream[IO, Swordfish] = Stream.emits(fishes).covary[IO]

  val whales = List(
    Whale("aaa", Random.nextInt()),
    Whale("bbb", Random.nextInt()),
    Whale("ccc", Random.nextInt())
  )
  val whaleStream: Stream[IO, Whale] = Stream.emits(whales).covary[IO]

  implicit val te: TypedEncoder[Swordfish] = shapeless.cachedImplicit
  implicit val rd: RowDecoder[Swordfish]   = shapeless.cachedImplicit
  implicit val re: RowEncoder[Swordfish]   = shapeless.cachedImplicit

}

class SingleFileTest extends AnyFunSuite {
  import SingleFileTestData._
  val sink: SingleFileSink[IO]          = fileSink[IO](blocker)
  val source: SingleFileSource[IO]      = fileSource[IO](blocker)
  def delete(path: String): IO[Boolean] = sink.delete(path)

  test("spark avro - identity") {
    val path = "./data/test/spark/singleFile/swordfish.snappy.avro"
    val run = delete(path) >>
      fishStream.through(sink.avro[Swordfish](path, CodecFactory.snappyCodec)).compile.drain >>
      source.avro[Swordfish](path).compile.toList
    assert(run.unsafeRunSync() === fishes)
  }

  test("spark avro-binary - identity") {
    val path = "./data/test/spark/singleFile/swordfish-binary.avro"
    val run = delete(path) >>
      fishStream.through(sink.binAvro[Swordfish](path)).compile.drain >>
      source.binAvro[Swordfish](path).compile.toList
    assert(run.unsafeRunSync() === fishes)

    //spark doesn't understand binary-avro
  }

  test("spark parquet - identity") {
    val path = "./data/test/spark/singleFile/swordfish.snappy.parquet"
    val run = delete(path) >>
      fishStream
        .through(sink.parquet[Swordfish](path, CompressionCodecName.SNAPPY))
        .compile
        .drain >>
      source.parquet[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)

  }

  test("spark circe json - identity") {
    val path = "./data/test/spark/singleFile/swordfish.json"
    val run = delete(path) >>
      fishStream.through(sink.circe[Swordfish](path)).compile.drain >>
      source.circe[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)
    assert(File(path).lineCount == 3L)

    //  val s = sparkSession.load.circe[Swordfish](path).typedDataset.collect[IO]().unsafeRunSync().toSet
    //  assert(s == fishes.toSet)

  }
  test("spark jackson - identity") {
    val path = "./data/test/spark/singleFile/swordfish-jackson.json"
    val run = delete(path) >>
      fishStream.through(sink.jackson[Swordfish](path)).compile.drain >>
      source.jackson[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)
    assert(File(path).lineCount == 3L)

    // val s =sparkSession.load.jackson[Swordfish](path).typedDataset.collect[IO]().unsafeRunSync().toSet
    // assert(s == fishes.toSet)
  }

  test("spark csv - identity") {
    val path = "./data/test/spark/singleFile/swordfish.csv"
    val run = delete(path) >>
      fishStream.through(sink.csv[Swordfish](path)).compile.drain >>
      source.csv[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)
    assert(File(path).lineCount == 3L)

  }

  test("spark csv with header - identity") {
    val path = "./data/test/spark/singleFile/swordfish-header.csv"
    val rfc = CsvConfiguration.rfc
      .withHeader("from", "weight", "code")
      .withCellSeparator('|')
      .withQuote('\"')
      .quoteAll

    val run = delete(path) >>
      fishStream.through(sink.csv[Swordfish](path, rfc)).compile.drain >>
      source.csv[Swordfish](path, rfc).compile.toList

    assert(run.unsafeRunSync() === fishes, "source")
    assert(File(path).lineCount == 4L)

  }

  test("spark java-object - identity") {
    val path = "./data/test/spark/singleFile/swordfish.obj"
    val run = delete(path) >>
      fishStream.through(sink.javaObject[Swordfish](path)).compile.drain >>
      source.javaObject[Swordfish](path).compile.toList
    assert(run.unsafeRunSync() === fishes)
  }

  test("spark protobuf - identity") {
    val path = "./data/test/spark/singleFile/whales.pb"
    val run = delete(path) >>
      whaleStream.through(sink.protobuf[Whale](path)).compile.drain >>
      source.protobuf[Whale](path).compile.toList
    assert(run.unsafeRunSync() === whales)
  }
}
