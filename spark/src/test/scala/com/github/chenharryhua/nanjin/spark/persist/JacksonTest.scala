package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.{HadoopJackson, NJHadoop, NJPath}
import com.sksamuel.avro4s.FromRecord
import eu.timepit.refined.auto.*
import fs2.Stream
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JacksonTest extends AnyFunSuite {

  def rooster(path: NJPath): SaveJackson[IO, Rooster] =
    new RddAvroFileHoarder[IO, Rooster](IO(RoosterData.rdd.repartition(3)), Rooster.avroCodec).jackson(path)

  val hdp: NJHadoop[IO]          = sparkSession.hadoop[IO]
  val jackson: HadoopJackson[IO] = hdp.jackson(Rooster.schema)

  val fromRecord: FromRecord[Rooster] = FromRecord(Rooster.avroCodec)

  def loadRooster(path: NJPath): IO[Set[Rooster]] =
    Stream
      .eval(hdp.filesIn(path))
      .flatMap(jackson.source(_).rethrow.map(fromRecord.from))
      .compile
      .toList
      .map(_.toSet)

  val root = NJPath("./data/test/spark/persist/jackson/")
  test("datetime read/write identity - uncompressed") {
    val path = root / "rooster" / "uncompressed"
    rooster(path).errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val r = loaders.rdd.jackson[Rooster](path, sparkSession, Rooster.avroCodec)
    assert(RoosterData.expected == r.collect().toSet)
    assert(RoosterData.expected == loadRooster(path).unsafeRunSync())
  }

  def bee(path: NJPath) =
    new RddAvroFileHoarder[IO, Bee](IO(BeeData.rdd.repartition(3)), Bee.avroCodec).jackson(path)

  test("byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = root / "bee" / "uncompressed"
    bee(path).uncompress.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.gzip") {
    import cats.implicits.*
    val path = root / "bee" / "gzip"
    bee(path).gzip.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.bzip2") {
    import cats.implicits.*
    val path = root / "bee" / "bzip2"
    bee(path).bzip2.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.deflate 9") {
    import cats.implicits.*
    val path = root / "bee" / "deflate9"
    bee(path).deflate(9).run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.lz4") {
    import cats.implicits.*
    val path = root / "bee" / "lz4"
    bee(path).lz4.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi.snappy") {
    import cats.implicits.*
    val path = root / "bee" / "snappy"
    bee(path).snappy.run.unsafeRunSync()
    val t = loaders.rdd.jackson[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("jackson jacket") {
    val path = NJPath("./data/test/spark/persist/jackson/jacket.json")
    val saver =
      new RddAvroFileHoarder[IO, Jacket](IO(JacketData.rdd.repartition(3)), Jacket.avroCodec).jackson(path)
    saver.run.unsafeRunSync()
    val t = loaders.rdd.jackson(path, sparkSession, Jacket.avroCodec)
    assert(JacketData.expected.toSet == t.collect().toSet)
  }

}
