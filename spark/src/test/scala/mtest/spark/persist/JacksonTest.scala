package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder}
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JacksonTest extends AnyFunSuite {

  val rooster =
    new RddAvroFileHoarder[IO, Rooster](RoosterData.rdd.repartition(3), Rooster.avroCodec.avroEncoder)

  test("datetime read/write identity - multi") {
    val path = "./data/test/spark/persist/jackson/rooster/multi.json"
    rooster.jackson(path).folder.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession)
    assert(RoosterData.expected == r.collect().toSet)
  }

  test("datetime read/write identity - single") {
    val path = "./data/test/spark/persist/jackson/rooster/single.json"
    rooster.jackson(path).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.jackson[Rooster](path, Rooster.ate, sparkSession).dataset
    assert(RoosterData.expected == r.collect().toSet)
    val t3 = loaders.stream
      .jackson[IO, Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession.sparkContext.hadoopConfiguration,100)
      .compile
      .toList
      .unsafeRunSync()
      .toSet

    assert(RoosterData.expected == t3)

  }

  val bee = new RddAvroFileHoarder[IO, Bee](BeeData.rdd.repartition(3), Bee.avroCodec.avroEncoder)
  test("byte-array read/write identity - single") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/single.json"
    bee.jackson(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/multi.json"
    bee.jackson(path).folder.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.gzip") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/multi.gzip.json"
    bee.jackson(path).folder.gzip.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.bzip2") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/multi.bzip2.json"
    bee.jackson(path).folder.bzip2.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.deflate") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/multi.deflate.json"
    bee.jackson(path).folder.deflate(9).run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - single.gzip") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/single.json.gz"
    bee.jackson(path).file.gzip.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - single.deflate") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/jackson/bee/single.json.deflate"
    bee.jackson(path).file.deflate(3).sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("jackson jacket") {
    val path  = "./data/test/spark/persist/jackson/jacket.json"
    val saver = new RddAvroFileHoarder[IO, Jacket](JacketData.rdd.repartition(3), Jacket.avroCodec.avroEncoder)
    saver.jackson(path).folder.run.unsafeRunSync()
    val t = loaders.rdd.jackson(path, Jacket.avroCodec.avroDecoder, sparkSession)
    assert(JacketData.expected.toSet == t.collect().toSet)
  }

  test("jackson fractual") {
    val path = "./data/test/spark/persist/jackson/fractual.json"
    val saver =
      new RddAvroFileHoarder[IO, Fractual](FractualData.rdd.repartition(3), Fractual.avroCodec.avroEncoder)
    saver.jackson(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.jackson[Fractual](path, Fractual.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(FractualData.data.toSet == t)
  }
}
