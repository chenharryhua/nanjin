package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.Json
import io.circe.generic.auto._
import org.apache.spark.rdd.RDD
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  val rooster = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd)

  test("rdd read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/multi.gzip"
    rooster.circe(path).folder.errorIfExists.ignoreIfExists.overwrite.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/multi.deflate"
    rooster.circe(path).folder.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed") {
    val path = "./data/test/spark/persist/circe/rooster/single.json"
    rooster.circe(path).file.uncompress.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
    val t3 = loaders.stream
      .circe[IO, Rooster](path, blocker, sparkSession.sparkContext.hadoopConfiguration)
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(RoosterData.expected == t3)
  }

  test("rdd read/write identity single.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.gz"
    rooster.circe(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.deflate"
    rooster.circe(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  val bee = new RddAvroFileHoarder[IO, Bee](BeeData.rdd, Bee.avroCodec.avroEncoder)
  test("byte-array rdd read/write identity multi bzip2") {
    val path = "./data/test/spark/persist/circe/bee/multi.bzip2.json"
    bee.circe(path).dropNull.folder.bzip2.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect.map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("byte-array rdd read/write identity single") {
    import BeeData._
    val path = "./data/test/spark/persist/circe/bee/single.json"
    bee.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect.map(_.toWasp).toSet === bees.map(_.toWasp).toSet)
  }

  test("rdd read/write identity multi.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed"
    rooster.circe(path).keepNull.folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/single.keepNull.uncompressed.json"
    rooster.circe(path).dropNull.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed - drop null") {
    val path = "./data/test/spark/persist/circe/rooster/single.dropNull.uncompressed.json"
    rooster.circe(path).dropNull.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("circe jacket") {
    val path  = "./data/test/spark/persist/circe/jacket.json"
    val saver = new RddFileHoarder[IO, Jacket](JacketData.ds.rdd)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Jacket](path, sparkSession).collect().toSet
    assert(JacketData.expected.toSet == t)
    val t2 = loaders.circe[Jacket](path, Jacket.ate, sparkSession).dataset.collect.toSet
    assert(JacketData.expected.toSet == t2)
  }

  test("circe jacket neck multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-single.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Neck](path, sparkSession).collect().toSet
    assert(data.map(_.value).toSet == t)
  }

  test("circe jacket neck json single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-json.json"
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe append") {
    val path  = "./data/test/spark/persist/circe/jacket-append.json"
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    val t1 =
      try loaders.rdd.circe[Json](path, sparkSession).count()
      catch { case _: Throwable => 0 }
    saver.circe(path).folder.append.run(blocker).unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert((data.size + t1) == t2)
  }

  test("circe fractual") {
    val path  = "./data/test/spark/persist/circe/fractual.json"
    val saver = new RddFileHoarder[IO, Fractual](FractualData.rdd)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Fractual](path, sparkSession).collect().toSet
    assert(FractualData.data.toSet == t)
  }

  test("circe primitive") {
    val path          = "./data/test/spark/persist/circe/primitive.json"
    val rdd: RDD[Int] = RoosterData.rdd.map(_.index)
    val expected      = rdd.collect().toSet
    rdd.save[IO].circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.circe[Int](path, AvroTypedEncoder[Int](AvroCodec[Int]), sparkSession).dataset.collect.toSet
    assert(expected == t)
  }
}
