package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder, RddFileHoarder}
import frameless.TypedDataset
import io.circe.Json
import io.circe.generic.auto.*
import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class CirceTest extends AnyFunSuite {

  val rooster = new RddFileHoarder[IO, Rooster](RoosterData.ds.rdd)

  test("circe rooster rdd read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/multi.gzip"
    rooster.circe(path).folder.errorIfExists.ignoreIfExists.overwrite.gzip.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe rooster rdd read/write identity multi.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/multi.deflate"
    rooster.circe(path).folder.deflate(3).run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed") {
    val path = "./data/test/spark/persist/circe/rooster/single.json"
    rooster.circe(path).file.uncompress.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
    val t3 = loaders.stream
      .circe[IO, Rooster](path, sparkSession.sparkContext.hadoopConfiguration,100)
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(RoosterData.expected == t3)
  }

  test("circe rooster rdd read/write identity single.gzip") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.gz"
    rooster.circe(path).file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.deflate") {
    val path = "./data/test/spark/persist/circe/rooster/single.json.deflate"
    rooster.circe(path).file.deflate(3).sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  val bee = new RddAvroFileHoarder[IO, Bee](BeeData.rdd.repartition(1), Bee.avroCodec.avroEncoder)
  test("circe bee byte-array rdd read/write identity multi bzip2") {
    val path = "./data/test/spark/persist/circe/bee/multi.bzip2.json"
    bee.circe(path).dropNull.folder.bzip2.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe bee byte-array rdd read/write identity multi uncompressed") {
    val path = "./data/test/spark/persist/circe/bee/multi.uncompressed.json"
    bee.circe(path).dropNull.folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe bee byte-array rdd read/write identity single gz") {
    val path = "./data/test/spark/persist/circe/bee/single.json.gz"
    bee.circe(path).file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe bee byte-array rdd read/write identity single uncompressed") {
    val path = "./data/test/spark/persist/circe/bee/single.json"
    bee.circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Bee](path, sparkSession)
    assert(t.collect().map(_.toWasp).toSet === BeeData.bees.map(_.toWasp).toSet)
  }

  test("circe rooster rdd read/write identity multi.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/multi.keepNull.uncompressed"
    rooster.circe(path).keepNull.folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed - keep null") {
    val path = "./data/test/spark/persist/circe/rooster/single.keepNull.uncompressed.json"
    rooster.circe(path).dropNull.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe rooster rdd read/write identity single.uncompressed - drop null") {
    val path = "./data/test/spark/persist/circe/rooster/single.dropNull.uncompressed.json"
    rooster.circe(path).dropNull.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Rooster](path, sparkSession)
    assert(RoosterData.expected == t.collect().toSet)
    val t2: TypedDataset[Rooster] = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(RoosterData.expected == t2.dataset.collect().toSet)
  }

  test("circe jacket") {
    val path  = "./data/test/spark/persist/circe/jacket.json"
    val saver = new RddFileHoarder[IO, Jacket](JacketData.ds.rdd)
    saver.circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Jacket](path, sparkSession).collect().toSet
    assert(JacketData.expected.toSet == t)
    val t2 = loaders.circe[Jacket](path, Jacket.ate, sparkSession).dataset.collect().toSet
    assert(JacketData.expected.toSet == t2)
  }

  test("circe jacket neck multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).folder.run.unsafeRunSync()
    val t = loaders.rdd.circe[KJson[Neck]](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-single.json"
    val data  = JacketData.expected.map(_.neck)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, KJson[Neck]](rdd.repartition(1))
    saver.circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Neck](path, sparkSession).collect().toSet
    assert(data.map(_.value).toSet == t)
  }

  test("circe jacket neck json single") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-json.json"
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Json](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }

  test("circe jacket neck json multi") {
    val path  = "./data/test/spark/persist/circe/jacket-neck-multi.json"
    val data  = JacketData.expected.map(_.neck.value.j)
    val rdd   = sparkSession.sparkContext.parallelize(data)
    val saver = new RddFileHoarder[IO, Json](rdd.repartition(1))
    saver.circe(path).folder.run.unsafeRunSync()
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
    saver.circe(path).folder.append.run.unsafeRunSync()
    val t2 = loaders.rdd.circe[Json](path, sparkSession).count()
    assert((data.size + t1) == t2)
  }

  test("circe fractual") {
    val path  = "./data/test/spark/persist/circe/fractual.json"
    val saver = new RddFileHoarder[IO, Fractual](FractualData.rdd)
    saver.circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.circe[Fractual](path, sparkSession).collect().toSet
    assert(FractualData.data.toSet == t)
  }

  test("circe primitive") {
    val path          = "./data/test/spark/persist/circe/primitive.json"
    val rdd: RDD[Int] = RoosterData.rdd.map(_.index)
    val expected      = rdd.collect().toSet
    rdd.save[IO].circe(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.circe[Int](path, AvroTypedEncoder[Int](AvroCodec[Int]), sparkSession).dataset.collect().toSet
    assert(expected == t)
  }
}
