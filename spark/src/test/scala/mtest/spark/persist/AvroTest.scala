package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroTest extends AnyFunSuite {

  test("datetime rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/raw"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.avro(path).raw.folder.run(blocker).unsafeRunSync()
    val r = loaders.raw.avro[Rooster](path).collect().toSet
    val t = loaders.avro[Rooster](path).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }

  test("datetime spark read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/spark"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Rooster](path).collect[IO]().unsafeRunSync().toSet
    assert(expected == t)
  }

  test("byte-array rdd read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/raw"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd)
    saver.avro(path).raw.folder.run(blocker).unsafeRunSync()
    val t = loaders.raw.avro[Bee](path).collect().toList
    val r = loaders.avro[Bee](path).collect[IO]().unsafeRunSync.toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array spark read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/spark.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd)
    saver.avro(path).spark.file.run(blocker).unsafeRunSync()
    val t = loaders.avro[Bee](path).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection raw read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/raw.avro"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd)
    saver.avro(path).raw.file.run(blocker).unsafeRunSync()
    val t = loaders.raw.avro[Ant](path).collect().toSet
    val r = loaders.avro[Ant](path).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("collection spark read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/spark"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t = loaders.avro[Ant](path).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
  }
}
