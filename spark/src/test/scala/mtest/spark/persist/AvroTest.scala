package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.AvroFileHoarder

@DoNotDiscover
class AvroTest extends AnyFunSuite {
  val hadoop = NJHadoop[IO](sparkSession.sparkContext.hadoopConfiguration, blocker)
  val gr     = new GenericRecordCodec[IO, Rooster]()

  def singleAvro(path: String): Set[Rooster] = hadoop
    .avroSource(path, Rooster.avroCodec.schema)
    .through(gr.decode(Rooster.avroCodec.avroDecoder))
    .compile
    .toList
    .unsafeRunSync()
    .toSet

  test("datetime read/write identity - multi.uncompressed") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.uncompressed.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(1), path, Rooster.avroCodec.avroEncoder)
    saver.avro.folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - multi.snappy") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.snappy.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.folder.snappy.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - multi.deflate") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.deflate.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(3), path, Rooster.avroCodec.avroEncoder)
    saver.avro.folder.deflate(3).run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - multi.xz") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.xz.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(4), path, Rooster.avroCodec.avroEncoder)
    saver.avro.folder.xz(3).run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - multi.bzip2") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.bzip2.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(5), path, Rooster.avroCodec.avroEncoder)
    saver.avro.folder.bzip2.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - single.uncompressed") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.uncompressed.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
    assert(expected == singleAvro(path))
  }

  test("datetime read/write identity - single.snappy") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.snappy.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.file.snappy.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
    assert(expected == singleAvro(path))
  }

  test("datetime read/write identity - single.bzip2") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.bzip2.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.file.bzip2.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
    assert(expected == singleAvro(path))
  }

  test("datetime read/write identity - single.deflate") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.deflate.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.file.deflate(5).run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
    assert(expected == singleAvro(path))
  }

  test("datetime read/write identity - single.xz") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.xz.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.avro.file.xz(5).run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
    assert(expected == singleAvro(path))
  }

  test("datetime spark write/nanjin read identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/spark-write.avro"
    val tds  = Rooster.ate.normalize(rdd)
    tds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("avroSchema", Rooster.schema.toString)
      .format("avro")
      .save(path)
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("byte-array read/write identity - multi") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/avro/bee/multi.raw"
    val saver = AvroFileHoarder[IO, Bee](rdd, path, Bee.avroCodec.avroEncoder)
    saver.avro.folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec).collect().toList
    val r = loaders.avro[Bee](path, Bee.ate).collect[IO]().unsafeRunSync.toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity single") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/avro/bee/single.raw.avro"
    val saver = AvroFileHoarder[IO, Bee](rdd, path, Bee.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val r = loaders.avro[Bee](path, Bee.ate).collect[IO]().unsafeRunSync.toList
    assert(bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /** the data saved to disk is correct.
    * loaders.rdd.avro can not rightly read it back.
    * loaders.avro can.
    * avro4s decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */
  test("byte-array read/write identity single - use customized codec") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/avro/bee/single2.raw.avro"
    val saver = AvroFileHoarder[IO, Bee](rdd, path, Bee.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec).collect().toList
    println(loaders.rdd.avro[Bee](path, Bee.avroCodec).map(_.toWasp).collect().toList)
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity single") {
    import AntData._
    val path  = "./data/test/spark/persist/avro/ant/single.raw.avro"
    val saver = AvroFileHoarder[IO, Ant](rdd, path, Ant.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Ant](path, Ant.avroCodec).collect().toSet
    val r = loaders.avro[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("collection read/write identity multi") {
    import AntData._
    val path  = "./data/test/spark/persist/avro/ant/multi.avro"
    val saver = AvroFileHoarder[IO, Ant](rdd, path, Ant.avroCodec.avroEncoder)
    saver.avro.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[Ant](path, Ant.avroCodec).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == t)
  }

  test("enum read/write identity single") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/emcop/single.avro"
    val saver = AvroFileHoarder[IO, EmCop](emRDD, path, EmCop.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("enum read/write identity multi") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/emcop/raw"
    val saver = AvroFileHoarder[IO, EmCop](emRDD, path, EmCop.avroCodec.avroEncoder)
    saver.avro.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("sealed trait read/write identity single/raw (happy failure)") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cocop/single.avro"
    val saver = AvroFileHoarder[IO, CoCop](coRDD, path, CoCop.codec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    intercept[Throwable](loaders.rdd.avro[CoCop](path, CoCop.codec).collect().toSet)
    // assert(coCops.toSet == t)
  }

  test("sealed trait read/write identity multi/raw (happy failure)") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cocop/multi.avro"
    val saver = AvroFileHoarder[IO, CoCop](coRDD, path, CoCop.codec.avroEncoder)
    intercept[Throwable](saver.avro.folder.run(blocker).unsafeRunSync())
    //  val t = loaders.raw.avro[CoCop](path).collect().toSet
    //  assert(coCops.toSet == t)
  }

  test("coproduct read/write identity - multi") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cpcop/multi.avro"
    val saver = AvroFileHoarder[IO, CpCop](cpRDD, path, CpCop.avroCodec.avroEncoder)
    saver.avro.folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("coproduct read/write identity - single") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cpcop/single.avro"
    val saver = AvroFileHoarder[IO, CpCop](cpRDD, path, CpCop.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("avro jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/avro/jacket.avro"
    val saver = AvroFileHoarder[IO, Jacket](rdd, path, Jacket.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Jacket](path, Jacket.avroCodec).collect().toSet
    assert(expected.toSet == t)
    val t2 = loaders.avro[Jacket](path, Jacket.ate).collect[IO]().unsafeRunSync.toSet
    assert(expected.toSet == t2)
  }

  test("avro fractual") {
    import FractualData._
    val path  = "./data/test/spark/persist/avro/fractual.avro"
    val saver = AvroFileHoarder[IO, Fractual](rdd, path, Fractual.avroCodec.avroEncoder)
    saver.avro.file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Fractual](path, Fractual.avroCodec).collect().toSet
    assert(data.toSet == t)
  }
}
