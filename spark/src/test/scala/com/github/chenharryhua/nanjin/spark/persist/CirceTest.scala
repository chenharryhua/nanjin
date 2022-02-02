package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJAvroCodec}
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.generic.auto.*
import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*
@DoNotDiscover
class CirceTest extends AnyFunSuite {

  def rooster(path: NJPath) = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd, HoarderConfig(path))

  test("circe rooster rdd read/write identity multi.gzip") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.gzip")
    rooster(path).circe.folder.errorIfExists.ignoreIfExists.overwrite.gzip.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe rooster rdd read/write identity multi.deflate") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.deflate")
    rooster(path).circe.folder.deflate(3).run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/single.json")
    rooster(path).circe.file.uncompress.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
    val t3 = loaders.stream
      .circe[IO, Rooster](path, sparkSession.sparkContext.hadoopConfiguration, 100.bytes)
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity single.gzip") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/single.json.gz")
    rooster(path).circe.file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.deflate") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/single.json.deflate")
    rooster(path).circe.file.deflate(3).sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  def bee(path: NJPath) =
    new RddAvroFileHoarder[IO, Bee](BeeData.rdd.repartition(1), Bee.avroCodec.avroEncoder, HoarderConfig(path))
  test("circe bee byte-array rdd read/write identity multi bzip2") {
    val path = NJPath("./data/test/spark/persist/circe/bee/multi.bzip2.json")
    bee(path).circe.dropNull.folder.bzip2.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe bee byte-array rdd read/write identity multi uncompressed") {
    val path = NJPath("./data/test/spark/persist/circe/bee/multi.uncompressed.json")
    bee(path).circe.dropNull.folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
    val s = loaders.stream
      .circe[IO, Bee](path, sparkSession.sparkContext.hadoopConfiguration, 100.bytes)
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(BeeData.bees.map(_.toWasp).toSet == s.map(_.toWasp))
  }

  test("circe bee byte-array rdd read/write identity single gz") {
    val path = NJPath("./data/test/spark/persist/circe/bee/single.json.gz")
    bee(path).circe.file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe bee byte-array rdd read/write identity single uncompressed") {
    val path = NJPath("./data/test/spark/persist/circe/bee/single.json")
    bee(path).circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe rooster rdd read/write identity multi.uncompressed - keep null") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed")
    rooster(path).circe.keepNull.folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed - keep null") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/single.keepNull.uncompressed.json")
    rooster(path).circe.dropNull.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed - drop null") {
    val path = NJPath("./data/test/spark/persist/circe/rooster/single.dropNull.uncompressed.json")
    rooster(path).circe.dropNull.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2 = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect().toSet)
  }

  test("circe jacket") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket.json")
    val saver = new RddFileHoarder[IO, Jacket](JacketData.ds.rdd, HoarderConfig(path))
    saver.circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Jacket](path, sparkSession).collect().toSet
    assert(JacketData.expected.toSet == t)
    val t2 = loaders.circe[Jacket](path, Jacket.ate, sparkSession).collect().toSet
    assert(JacketData.expected.toSet == t2)
  }

  test("circe jacket neck multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1), HoarderConfig(path))
    saver.circe.folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck single") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-single.json")
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1), HoarderConfig(path))
    saver.circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Neck](path, sparkSession).collect().toSet
    assert(data.map(_.value).toSet == t)
  }

  test("circe jacket neck json single") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-json.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1), HoarderConfig(path))
    saver.circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = NJPath("./data/test/spark/persist/circe/jacket-neck-multi.json")
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1), HoarderConfig(path))
    saver.circe.folder.run.unsafeRunSync()
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
    saver.circe.folder.append.run.unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert((data.size + t1) == t2)
  }

  test("circe fractual") {
    val path  = NJPath("./data/test/spark/persist/circe/fractual.json")
    val saver = new RddFileHoarder[IO, Fractual](FractualData.rdd, HoarderConfig(path))
    saver.circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Fractual](path, sparkSession).collect().toSet
    assert(FractualData.data.toSet == t)
  }

  test("circe primitive") {
    val path          = NJPath("./data/test/spark/persist/circe/primitive.json")
    val rdd: RDD[Int] = RoosterData.rdd.map(_.index)
    val expected      = rdd.collect().toSet
    rdd.save[IO](path).circe.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.circe[Int](path, AvroTypedEncoder[Int](NJAvroCodec[Int]), sparkSession).collect().toSet
    assert(expected == t)
  }
}
