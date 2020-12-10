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

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  val rooster = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd)

  test("rdd read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/multi.gzip"
    rooster.circe(path).folder.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/multi.deflate"
    rooster.circe(path).folder.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed") {
    val path = "./data/test/spark/persist/circe/rooster/single.json"
    rooster.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.gz"
    rooster.circe(path).file.gzip.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.deflate"
    rooster.circe(path).file.deflate(3).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  val bee = new RddAvroFileHoarder[IO, Bee](BeeData.rdd, Bee.avroCodec.avroEncoder)
  test("byte-array rdd read/write identity multi") {
    val path = "./data/test/spark/persist/circe/bee/multi.json"
    bee.circe(path).folder.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path)
    assert(t.collect.map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("byte-array rdd read/write identity single") {
    import BeeData._
    val path = "./data/test/spark/persist/circe/bee/single.json"
    bee.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path)
    assert(t.collect.map(_.toWasp).toSet === bees.map(_.toWasp).toSet)
  }

  test("rdd read/write identity multi.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed"
    rooster.circe(path).folder.keepNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity multi.uncompressed - drop null") {
    val path = "./data/test/spark/persist/circe/rooster/multi.dropNull.uncompressed"
    rooster.circe(path).folder.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/single.keepNull.uncompressed.json"
    rooster.circe(path).file.keepNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("rdd read/write identity single.uncompressed - drop null") {
    val path = "./data/test/spark/persist/circe/rooster/single.dropNull.uncompressed.json"
    rooster.circe(path).file.dropNull.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate)
    assert(RoosterData.expected == t2.collect[IO]().unsafeRunSync().toSet)
  }

  test("circe jacket") {
    val path  = "./data/test/spark/persist/circe/jacket.json"
    val saver = new RddFileHoarder[IO, Jacket](JacketData.ds.rdd)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Jacket](path).collect().toSet
    assert(JacketData.expected.toSet == t)
    val t2 = loaders.circe[Jacket](path, Jacket.ate).collect[IO]().unsafeRunSync().toSet
    assert(JacketData.expected.toSet == t2)
  }

  test("circe jacket neck multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-single.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Neck](path).collect().toSet
    assert(data.map(_.value).toSet == t)
  }

  test("circe jacket neck json single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-json.json"
    val data  = JacketData.expected.map(_.neck.value.b)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Json](path).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck.value.b)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Json](path).collect().toSet
    assert(data.toSet == t)
  }

  test("circe fractual") {
    val path  = "./data/test/spark/persist/circe/fractual.json"
    val saver = new RddFileHoarder[IO, Fractual](FractualData.rdd)
    saver.circe(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.circe[Fractual](path).collect().toSet
    assert(FractualData.data.toSet == t)
  }

  test("circe primitive") {
    val path          = "./data/test/spark/persist/circe/primitive.json"
    val rdd: RDD[Int] = RoosterData.rdd.map(_.index)
    val expected      = rdd.collect().toSet
    rdd.save[IO].circe(path).file.run(blocker).unsafeRunSync()
    val t =
      loaders
        .circe[Int](path, AvroTypedEncoder[Int](AvroCodec[Int]))
        .collect[IO]()
        .unsafeRunSync()
        .toSet

    assert(expected == t)
  }
}
