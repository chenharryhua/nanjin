package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.{HadoopBinAvro, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import mtest.spark.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {
  val hdp                         = sparkSession.hadoop[IO]
  val bin_avro: HadoopBinAvro[IO] = hdp.binAvro(Rooster.schema)

  def saver(path: NJPath) =
    new RddAvroFileHoarder[IO, Rooster](IO(RoosterData.rdd.repartition(2)), Rooster.avroCodec.avroEncoder)
      .binAvro(path)
      .overwrite

  def loadRooster(path: NJPath): IO[Set[Rooster]] =
    Stream
      .eval(hdp.filesIn(path))
      .flatMap(bin_avro.source)
      .map(Rooster.avroCodec.fromRecord)
      .compile
      .toList
      .map(_.toSet)

  val root = NJPath("./data/test/spark/persist/bin_avro/") / "rooster"
  test("binary avro - uncompressed") {
    val path = root / "rooster" / "uncompressed"
    saver(path).uncompress.append.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val r3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == r3)
  }

  test("binary avro - gzip") {
    val path = root / "gzip"
    saver(path).gzip.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - lz4") {
    val path = root / "lz4"
    saver(path).lz4.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - bzip2") {
    val path = root / "bzip2"
    saver(path).bzip2.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate2") {
    val path = root / "deflate2"
    saver(path).deflate(2).run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate-2") {
    val path = root / "deflate-2"
    saver(path).deflate(2).run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - snappy") {
    val path = root / "snappy"
    saver(path).snappy.run.unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  val reverseRoot = root / "reverse"
  test("reverse read/write gzip") {
    val path = reverseRoot / "rooster.binary.avro.gz"
    IO(RoosterData.rdd).output
      .stream(100)
      .map(Rooster.avroCodec.toRecord)
      .chunks
      .through(bin_avro.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    assert(RoosterData.expected == t1)
  }
  test("reverse read/write bzip2") {
    val path = reverseRoot / "rooster.binary.avro.bz2"
    IO(RoosterData.rdd).output
      .stream(100)
      .map(Rooster.avroCodec.toRecord)
      .chunks
      .through(bin_avro.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    assert(RoosterData.expected == t1)
  }

  test("reverse read/write uncompress") {
    val path = reverseRoot / "rooster.binary.avro"
    IO(RoosterData.rdd).output
      .stream(100)
      .map(Rooster.avroCodec.toRecord)
      .chunks
      .through(bin_avro.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    assert(RoosterData.expected == t1)
  }

  test("ftp") {
    val path = NJPath("ftp://localhost/data2/bin_avro.avro")
    IO(RoosterData.rdd).output
      .stream(100)
      .map(Rooster.avroCodec.toRecord)
      .chunks
      .through(bin_avro.sink(path))
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec.avroDecoder).collect().toSet
    assert(RoosterData.expected == t1)
  }
}
