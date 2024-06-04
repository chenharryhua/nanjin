package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.{HadoopCirce, NJPath}
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.generic.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  def rooster(path: NJPath)  = new RddFileHoarder[Rooster](RoosterData.ds.rdd).circe(path)
  val hdp                    = sparkSession.hadoop[IO]
  val circe: HadoopCirce[IO] = hdp.circe

  def loadRoosters(path: NJPath): IO[List[Rooster]] =
    fs2.Stream
      .eval(hdp.filesIn(path))
      .flatMap(circe.source(_, 2))
      .map(_.flatMap(_.as[Rooster]))
      .rethrow
      .compile
      .toList

  def loadBees(path: NJPath): IO[List[Bee]] =
    fs2.Stream
      .eval(hdp.filesIn(path))
      .flatMap(circe.source(_, 2))
      .map(_.flatMap(_.as[Bee]))
      .rethrow
      .compile
      .toList

  val root = NJPath("./data/test/spark/persist/circe")

  test("circe rooster rdd read/write identity multi.uncompressed") {
    val path = root / "rooster" / "uncompressed"
    rooster(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.gzip") {
    val path = root / "rooster" / "gzip"
    rooster(path).withCompression(_.Gzip).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.deflate 3") {
    val path = root / "rooster" / "deflate3"
    rooster(path).withCompression(_.Deflate(3)).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.lz4") {
    val path = root / "rooster" / "lz4"
    rooster(path).withCompression(_.Lz4).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.snappy") {
    val path = root / "rooster" / "snappy"
    rooster(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  def bee(path: NJPath) =
    new RddAvroFileHoarder[Bee](BeeData.rdd.repartition(1), Bee.avroCodec).circe(path)

  test("circe bee byte-array rdd read/write identity multi bzip2") {
    val path = root / "bee" / "bzip2"
    bee(path).dropNull.withCompression(_.Bzip2).run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe bee byte-array rdd read/write identity multi uncompressed") {
    val path = root / "bee" / "uncompressed" / "drop_null"
    bee(path).dropNull.run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe rooster rdd read/write identity multi.uncompressed - keep null") {
    val path = root / "bee" / "uncompressed" / "keep_null"
    rooster(path).keepNull.run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.spark.json[Rooster](path, sparkSession, Rooster.ate)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe jacket neck multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[KJson[Neck]](rdd.repartition(1)).circe(path)
    saver.run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[Json](rdd.repartition(1)).circe(path)
    saver.run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe append") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-append.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[Json](rdd.repartition(1)).circe(path)
    val t1 =
      try loaders.rdd.circe[Json](path, sparkSession).count()
      catch { case _: Throwable => 0 }
    saver.withSaveMode(_.Append).run[IO].unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert(data.size + t1 == t2)
  }

  test("kjson") {
    val path  = NJPath("./data/test/spark/persist/circe/kjson.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data).map(KJson(_))
    val saver = new RddFileHoarder[KJson[Json]](rdd.repartition(1)).circe(path)
    saver.run[IO].unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
