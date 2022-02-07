package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.github.chenharryhua.nanjin.spark.*
import mtest.spark.*
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

@DoNotDiscover
class AvroTest extends AnyFunSuite {
  val hadoop: NJHadoop[IO] = sparkSession.hadoop[IO]

  def singleAvro(path: NJPath): Set[Rooster] =
    hadoop
      .avroSource(path, Rooster.avroCodec.schema, 100)
      .map(Rooster.avroCodec.avroDecoder.decode)
      .compile
      .toList
      .unsafeRunSync()
      .toSet

  def rooster(path: NJPath) =
    new RddAvroFileHoarder[IO, Rooster](
      RoosterData.ds.rdd.repartition(3),
      Rooster.avroCodec.avroEncoder,
      HoarderConfig(path))

  test("datetime read/write identity - multi.uncompressed") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.uncompressed.avro")
    rooster(path).avro.uncompress.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.snappy") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.snappy.avro")
    rooster(path).avro.snappy.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.deflate") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.deflate.avro")
    rooster(path).avro.deflate(3).folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.xz") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.xz.avro")
    rooster(path).avro.xz(3).folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.bzip2") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.bzip2.avro")
    rooster(path).avro.bzip2.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - single.uncompressed") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/single.uncompressed.avro")
    rooster(path).avro.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))

  }

  test("datetime read/write identity - single.snappy") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/single.snappy.avro")
    rooster(path).avro.snappy.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.bzip2") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/single.bzip2.avro")
    rooster(path).avro.bzip2.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.deflate") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/single.deflate.avro")
    rooster(path).avro.deflate(5).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.xz") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/single.xz.avro")
    rooster(path).avro.xz(5).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime spark write/nanjin read identity") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/spark-write.avro")
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("avroSchema", Rooster.schema.toString)
      .format("avro")
      .save(path.pathStr)
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime spark write without schema/nanjin read identity") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/spark-write-no-schema.avro")
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds.repartition(1).write.mode(SaveMode.Overwrite).format("avro").save(path.pathStr)
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t)
  }

  def bee(path: NJPath) = new RddAvroFileHoarder[IO, Bee](BeeData.rdd, Bee.avroCodec.avroEncoder, HoarderConfig(path))
  test("byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/avro/bee/multi.raw")
    bee(path).avro.folder.run.unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    val r = loaders.avro[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity single") {
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/avro/bee/single.raw.avro")
    bee(path).avro.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.avro[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /** the data saved to disk is correct. loaders.rdd.avro can not rightly read it back. loaders.avro can. avro4s
    * decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */

  test("byte-array read/write identity single - use customized codec") {
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/avro/bee/single2.raw.avro")
    bee(path).avro.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    println(loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).map(_.toWasp).collect().toList)
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  def ant(path: NJPath) = new RddAvroFileHoarder[IO, Ant](AntData.rdd, Ant.avroCodec.avroEncoder, HoarderConfig(path))
  test("collection read/write identity single") {
    import AntData.*
    val path = NJPath("./data/test/spark/persist/avro/ant/single.raw.avro")
    ant(path).avro.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Ant](path, Ant.avroCodec.avroDecoder, sparkSession).collect().toSet
    val r = loaders.avro[Ant](path, Ant.ate, sparkSession).collect().toSet
    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("collection read/write identity multi") {
    import AntData.*
    val path = NJPath("./data/test/spark/persist/avro/ant/multi.avro")
    ant(path).avro.folder.run.unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate, sparkSession).collect().toSet
    val r = loaders.rdd.avro[Ant](path, Ant.avroCodec.avroDecoder, sparkSession).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == t)
  }

  test("enum read/write identity single") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/emcop/single.avro")
    val saver = new RddAvroFileHoarder[IO, EmCop](emRDD, EmCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("enum read/write identity multi") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/emcop/raw")
    val saver = new RddAvroFileHoarder[IO, EmCop](emRDD, EmCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.folder.run.unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("sealed trait read/write identity single/raw (happy failure)") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cocop/single.avro")
    val saver = new RddAvroFileHoarder[IO, CoCop](coRDD, CoCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.file.sink.compile.drain.unsafeRunSync()
    intercept[Throwable](loaders.rdd.avro[CoCop](path, CoCop.avroCodec.avroDecoder, sparkSession).collect().toSet)
    // assert(coCops.toSet == t)
  }

  test("sealed trait read/write identity multi/raw (happy failure)") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cocop/multi.avro")
    val saver = new RddAvroFileHoarder[IO, CoCop](coRDD, CoCop.avroCodec.avroEncoder, HoarderConfig(path))
    intercept[Throwable](saver.avro.folder.run.unsafeRunSync())
    //  val t = loaders.raw.avro[CoCop](path).collect().toSet
    //  assert(coCops.toSet == t)
  }

  test("coproduct read/write identity - multi") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cpcop/multi.avro")
    val saver = new RddAvroFileHoarder[IO, CpCop](cpRDD, CpCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.folder.run.unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("coproduct read/write identity - single") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cpcop/single.avro")
    val saver = new RddAvroFileHoarder[IO, CpCop](cpRDD, CpCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("avro jacket") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/avro/jacket.avro")
    val saver = new RddAvroFileHoarder[IO, Jacket](rdd, Jacket.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Jacket](path, Jacket.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(expected.toSet == t)
    val t2 = loaders.avro[Jacket](path, Jacket.ate, sparkSession).collect().toSet
    assert(expected.toSet == t2)
  }

  test("avro fractual") {
    import FractualData.*
    val path  = NJPath("./data/test/spark/persist/avro/fractual.avro")
    val saver = new RddAvroFileHoarder[IO, Fractual](rdd, Fractual.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.file.sink.compile.drain.unsafeRunSync()
    val t =
      loaders.rdd.avro[Fractual](path, Fractual.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
