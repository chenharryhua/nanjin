package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJAvroCodec}
import com.github.chenharryhua.nanjin.pipes.BinaryAvroSerde
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.Json
import mtest.spark.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {

  def saver(path: NJPath) =
    new RddAvroFileHoarder[IO, Rooster](RoosterData.rdd.repartition(2), Rooster.avroCodec.avroEncoder)
      .binAvro(path)
      .overwrite

  def loadRooster(path: NJPath) = fs2.Stream
    .force(
      hdp
        .filesSortByName(path)
        .map(is =>
          is.foldLeft(Stream.empty.covaryAll[IO, Rooster]) { case (ss, hif) =>
            ss ++ hdp.bytes
              .source(hif)
              .through(BinaryAvroSerde.fromBytes(Rooster.schema))
              .map(Rooster.avroCodec.fromRecord)
          }))
    .compile
    .toList
    .map(_.toSet)

  val hdp = sparkSession.hadoop[IO]

  val root = NJPath("./data/test/spark/persist/bin_avro/") / "rooster"
  test("binary avro - uncompressed") {
    val path = root / "rooster" / "uncompressed"
    saver(path).uncompress.append.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val r3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == r3)
  }

  test("binary avro - gzip") {
    val path = root / "gzip"
    saver(path).gzip.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - lz4") {
    val path = root / "lz4"
    saver(path).lz4.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - bzip2") {
    val path = root / "bzip2"
    saver(path).bzip2.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate") {
    val path = root / "deflate"
    saver(path).deflate(2).run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - snappy") {
    val path = root / "snappy"
    saver(path).snappy.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  val reverseRoot = root / "reverse"
  test("reverse read/write gzip") {
    val path = reverseRoot / "rooster.binary.avro.gz"
    RoosterData.rdd
      .stream[IO](100)
      .map(Rooster.avroCodec.toRecord)
      .through(BinaryAvroSerde.toBytes[IO](Rooster.schema))
      .through(hdp.bytes.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
  }
  test("reverse read/write bzip2") {
    val path = reverseRoot / "rooster.binary.avro.bz2"
    RoosterData.rdd
      .stream[IO](100)
      .map(Rooster.avroCodec.toRecord)
      .through(BinaryAvroSerde.toBytes[IO](Rooster.schema))
      .through(hdp.bytes.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
  }

  test("reverse read/write uncompress") {
    val path = reverseRoot / "rooster.binary.avro"
    RoosterData.rdd
      .stream[IO](100)
      .map(Rooster.avroCodec.toRecord)
      .through(BinaryAvroSerde.toBytes[IO](Rooster.schema))
      .through(hdp.bytes.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
  }

  ignore("local") {
    val path =
      NJPath("/Users/chenh/Downloads/part-r-00000-9ed5fd81-6b1f-3708-a6a2-4d9c2392df33.binary.avro.bz2")
    val codec = NJAvroCodec[NJConsumerRecord[KJson[Json], KJson[Json]]]
    loaders.rdd.binAvro(path, codec.avroDecoder, sparkSession).count()
  }
}
