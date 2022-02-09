package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import com.github.chenharryhua.nanjin.pipes.serde.CirceSerde
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.generic.auto.*
import mtest.spark.*
import org.apache.hadoop.io.compress.{CompressionCodec, DeflateCodec, GzipCodec}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  def rooster(path: NJPath) = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd, HoarderConfig(path))
  val hdp                   = sparkSession.hadoop[IO]
  def loadRoosters(path: NJPath, codec: Option[CompressionCodec]) = {
    val rst =
      hdp
        .filesByName(path)
        .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Rooster]) { case (ss, hif) =>
          ss ++ hdp.bytes.withCompressionCodec(codec).source(hif).through(CirceSerde.deserPipe[IO, Rooster])
        })
    fs2.Stream.force(rst).compile.toList
  }
  def loadBees(path: NJPath) = {
    val rst =
      hdp
        .filesByName(path)
        .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Bee]) { case (ss, hif) =>
          hdp.bytes.source(hif).through(CirceSerde.deserPipe[IO, Bee])
        })
    fs2.Stream.force(rst).compile.toList
  }

  test("circe rooster rdd read/write identity multi.gzip") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.gzip")
    rooster(path).circe.errorIfExists.ignoreIfExists.overwrite.gzip.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path, Some(new GzipCodec())).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity multi.deflate") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.deflate")
    rooster(path).circe.deflate(3).run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loadRoosters(path, Some(new DeflateCodec())).unsafeRunSync().toSet
    assert(RoosterData.expected == t3)
  }

  def bee(path: NJPath) =
    new RddAvroFileHoarder[IO, Bee](BeeData.rdd.repartition(1), Bee.avroCodec.avroEncoder, HoarderConfig(path))

  test("circe bee byte-array rdd read/write identity multi bzip2") {
    val path = NJPath("./data/test/spark/persist/circe/bee/multi.bzip2.json")
    bee(path).circe.dropNull.bzip2.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe bee byte-array rdd read/write identity multi uncompressed") {
    val path = NJPath("./data/test/spark/persist/circe/bee/multi.uncompressed.json")
    bee(path).circe.dropNull.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet == BeeData.bees.map(_.toWasp).toSet)
    val t3 = loadBees(path).unsafeRunSync().toSet
    assert(BeeData.bees.map(_.toWasp).toSet == t3.map(_.toWasp))
  }

  test("circe rooster rdd read/write identity multi.uncompressed - keep null") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed")
    rooster(path).circe.keepNull.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe jacket neck multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1), HoarderConfig(path))
    saver.circe.run.unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1), HoarderConfig(path))
    saver.circe.run.unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe append") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-append.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1), HoarderConfig(path))
    val t1 =
      try loaders.rdd.circe[Json](path, sparkSession).count()
      catch { case _: Throwable => 0 }
    saver.circe.append.run.unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert((data.size + t1) == t2)
  }

}
