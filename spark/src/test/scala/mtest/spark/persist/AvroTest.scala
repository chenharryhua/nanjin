package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroTest extends AnyFunSuite {

  test("datetime read/write identity - multi/raw") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.raw"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.avro(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime read/write identity - multi/spark") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/multi.spark"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == t)
  }

  test("datetime read/write identity - single") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/single.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("byte-array read/write identity - multi/raw") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/multi.raw"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec)
    saver.avro(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.codec).collect().toList
    val r = loaders.avro[Bee](path, Bee.ate).collect[IO]().unsafeRunSync.toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity - multi/spark") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/spark.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Bee](path, Bee.ate).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity single") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/single.raw.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val r = loaders.avro[Bee](path, Bee.ate).collect[IO]().unsafeRunSync.toList
    assert(bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /**
    * the data saved to disk is correct.
    * loaders.rdd.avro can not rightly read it back.
    * loaders.avro can.
    * avro4s decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */
  test("byte-array read/write identity single - use customized codec") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/single2.raw.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(1)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.codec).collect().toList
    println(loaders.rdd.avro[Bee](path, Bee.codec).map(_.toWasp).collect().toList)
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity single") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/single.raw.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd, Ant.codec)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[Ant](path, Ant.codec).collect().toSet
    val r = loaders.avro[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("collection read/write identity multi/spark") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/spark.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd, Ant.codec)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
  }
  test("collection read/write identity multi/raw") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/multi.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd, Ant.codec)
    saver.avro(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[Ant](path, Ant.codec).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == t)

  }
  test("enum read/write identity single") {
    import CopData._
    val path = "./data/test/spark/persist/avro/emcop/single.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, EmCop](emRDD, EmCop.codec)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.codec).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }
  test("enum read/write identity multi/spark") {
    import CopData._
    val path = "./data/test/spark/persist/avro/emcop/spark"
    delete(path)
    val saver = new RddFileHoarder[IO, EmCop](emRDD, EmCop.codec)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate).collect[IO]().unsafeRunSync().toSet
    assert(emCops.toSet == t)
  }
  test("enum read/write identity multi/raw") {
    import CopData._
    val path = "./data/test/spark/persist/avro/emcop/raw"
    delete(path)
    val saver = new RddFileHoarder[IO, EmCop](emRDD, EmCop.codec)
    saver.avro(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate).collect[IO]().unsafeRunSync().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.codec).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("sealed trait read/write identity single/raw (happy failure)") {
    import CopData._
    val path = "./data/test/spark/persist/avro/cocop/single.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, CoCop](coRDD, CoCop.codec)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    intercept[Throwable](loaders.rdd.avro[CoCop](path, CoCop.codec).collect().toSet)
    // assert(coCops.toSet == t)
  }
  test("sealed trait read/write identity multi/raw (happy failure)") {
    import CopData._
    val path = "./data/test/spark/persist/avro/cocop/multi.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, CoCop](coRDD, CoCop.codec)
    intercept[Throwable](saver.avro(path).folder.run(blocker).unsafeRunSync())
    //  val t = loaders.raw.avro[CoCop](path).collect().toSet
    //  assert(coCops.toSet == t)
  }
  test("coproduct read/write identity - multi/raw") {
    import CopData._
    val path = "./data/test/spark/persist/avro/cpcop/multi"
    delete(path)
    val saver = new RddFileHoarder[IO, CpCop](cpRDD, CpCop.codec)
    saver.avro(path).folder.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.codec).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("coproduct read/write identity - single/raw") {
    import CopData._
    val path = "./data/test/spark/persist/avro/cpcop/single.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, CpCop](cpRDD, CpCop.codec)
    saver.avro(path).file.run(blocker).unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.codec).collect().toSet
    assert(cpCops.toSet == t)
  }
}
