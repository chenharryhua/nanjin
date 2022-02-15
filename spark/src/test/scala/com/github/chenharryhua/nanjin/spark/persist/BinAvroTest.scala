package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.pipes.BinaryAvroSerde
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
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

  test("binary avro - uncompressed") {
    val path = NJPath("./data/test/spark/persist/bin_avro/bin.avro.uncompressed")
    saver(path).append.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val r3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == r3)
  }

  test("binary avro - gzip") {
    val path = NJPath("./data/test/spark/persist/bin_avro/bin.avro.gzip")
    saver(path).gzip.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - snappy") {
    val path = NJPath("./data/test/spark/persist/bin_avro/bin.avro.lz4")
    saver(path).lz4.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - bzip2") {
    val path = NJPath("./data/test/spark/persist/bin_avro/bin.avro.bzip2")
    saver(path).bzip2.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate") {
    val path = NJPath("./data/test/spark/persist/bin_avro/bin.avro.deflate")
    saver(path).deflate(2).run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("reverse read/write gzip") {
    val path = NJPath("./data/test/spark/persist/bin_avro/reverse.bin.avro.gz")
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
    val path = NJPath("./data/test/spark/persist/bin_avro/reverse.bin.avro.bz2")
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
    val path = NJPath("./data/test/spark/persist/bin_avro/reverse.uncompress.bin.avro")
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
}
