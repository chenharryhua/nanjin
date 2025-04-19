package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.ToRecord
import eu.timepit.refined.auto.*
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {
  val hdp: Hadoop[IO] = sparkSession.hadoop[IO]

  def saver(path: Url): SaveBinaryAvro[Rooster] =
    new RddAvroFileHoarder[Rooster](RoosterData.rdd.repartition(2), Rooster.avroCodec)
      .binAvro(path)
      .withSaveMode(_.Overwrite)

  def loadRooster(path: Url): IO[Set[Rooster]] =
    hdp
      .filesIn(path)
      .flatMap(_.flatTraverse(
        hdp.source(_).binAvro(100, Rooster.schema).map(Rooster.avroCodec.decode).compile.toList))
      .map(_.toSet)

  val root = "./data/test/spark/persist/bin_avro/" / "rooster"
  test("binary avro - uncompressed") {
    val path = root / "rooster" / "uncompressed"
    saver(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val r3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == r3)
  }

  test("binary avro - gzip") {
    val path = root / "gzip"
    saver(path).withCompression(_.Gzip).run[IO].unsafeRunSync()
    val t1 = sparkSession.loadRdd[Rooster](path).binAvro(Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - lz4") {
    val path = root / "lz4"
    saver(path).withCompression(_.Lz4).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - bzip2") {
    val path = root / "bzip2"
    saver(path).withCompression(_.Bzip2).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate2") {
    val path = root / "deflate2"
    saver(path).withCompression(_.Deflate(2)).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }
  test("binary avro - deflate-2") {
    val path = root / "deflate-2"
    saver(path).withCompression(_.Deflate(2)).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  test("binary avro - snappy") {
    val path = root / "snappy"
    saver(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t2 = loaders.binAvro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t1)
    assert(RoosterData.expected == t2)
    val t3 = loadRooster(path).unsafeRunSync()
    assert(RoosterData.expected == t3)
  }

  val reverseRoot                 = root / "reverse"
  val toRecord: ToRecord[Rooster] = ToRecord(Rooster.avroCodec)
  test("reverse read/write gzip") {
    val path = reverseRoot / "rooster.binary.avro.gz"
    Stream
      .fromBlockingIterator[IO]
      .apply(RoosterData.rdd.toLocalIterator, 100)
      .map(toRecord.to)
      .chunks
      .through(hdp.sink(path).binAvro)
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    assert(RoosterData.expected == t1)
  }
  test("reverse read/write bzip2") {
    val path = reverseRoot / "rooster.binary.avro.bz2"
    Stream
      .fromBlockingIterator[IO]
      .apply(RoosterData.rdd.toLocalIterator, 100)
      .map(toRecord.to)
      .chunks
      .through(hdp.sink(path).binAvro)
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    assert(RoosterData.expected == t1)
  }

  test("reverse read/write uncompress") {
    val path = reverseRoot / "rooster.binary.avro"
    Stream
      .fromBlockingIterator[IO]
      .apply(RoosterData.rdd.toLocalIterator, 100)
      .map(toRecord.to)
      .chunks
      .through(hdp.sink(path).binAvro)
      .compile
      .drain
      .unsafeRunSync()

    val t1 = loaders.rdd.binAvro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    assert(RoosterData.expected == t1)
  }
}
