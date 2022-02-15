package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import com.github.chenharryhua.nanjin.pipes.CirceSerde
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.generic.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  def rooster(path: NJPath) = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd).circe(path)
  val hdp                   = sparkSession.hadoop[IO]
  def loadRoosters(path: NJPath) = {
    val rst =
      hdp
        .filesSortByName(path)
        .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Rooster]) { case (ss, hif) =>
          ss ++ hdp.bytes.source(hif).through(CirceSerde.fromBytes[IO, Rooster])
        })
    fs2.Stream.force(rst).compile.toList
  }
  def loadBees(path: NJPath) = {
    val rst =
      hdp
        .filesSortByName(path)
        .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Bee]) { case (ss, hif) =>
          hdp.bytes.source(hif).through(CirceSerde.fromBytes[IO, Bee])
        })
    fs2.Stream.force(rst).compile.toList
  }

  val root = NJPath("./data/test/spark/persist/circe")
  test("circe rooster rdd read/write identity multi.gzip") {
    val path = root / "rooster" / "gzip"
    rooster(path).errorIfExists.ignoreIfExists.overwrite.gzip.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.deflate") {
    val path = root / "rooster" / "deflate"
    rooster(path).deflate(3).run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.lz4") {
    val path = root / "rooster" / "lz4"
    rooster(path).lz4.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  def bee(path: NJPath) =
    new RddAvroFileHoarder[IO, Bee](BeeData.rdd.repartition(1), Bee.avroCodec.avroEncoder).circe(path)

  test("circe bee byte-array rdd read/write identity multi bzip2") {
    val path = root / "bee" / "bzip2"
    bee(path).dropNull.bzip2.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe bee byte-array rdd read/write identity multi uncompressed") {
    val path = root / "bee" / "uncompressed" / "drop_null"
    bee(path).dropNull.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe rooster rdd read/write identity multi.uncompressed - keep null") {
    val path = root / "bee" / "uncompressed" / "keep_null"
    rooster(path).keepNull.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe jacket neck multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1)).circe(path)
    saver.run.unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1)).circe(path)
    saver.run.unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe append") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-append.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1)).circe(path)
    val t1 =
      try loaders.rdd.circe[Json](path, sparkSession).count()
      catch { case _: Throwable => 0 }
    saver.append.run.unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert((data.size + t1) == t2)
  }

}
