package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoader}
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
    val saver = new RddFileHoader[IO, Rooster](rdd)
    saver.avro(path).raw.folder.run(blocker).unsafeRunSync()
    val r: RDD[Rooster]          = loaders.raw.avro[Rooster](path)
    val t: TypedDataset[Rooster] = loaders.avro[Rooster](path)
    assert(expected == r.collect().toSet)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("datetime spark read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/spark"
    delete(path)
    val saver = new RddFileHoader[IO, Rooster](rdd)
    saver.avro(path).spark.folder.run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.avro[Rooster](path)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("byte-array rdd read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/raw"
    delete(path)
    val saver = new RddFileHoader[IO, Bee](rdd)
    saver.avro(path).raw.folder.run(blocker).unsafeRunSync()
    val t = loaders.raw.avro[Bee](path)
    assert(bees.sortBy(_.b).zip(t.collect().toList.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array spark read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/spark.avro"
    delete(path)
    val saver = new RddFileHoader[IO, Bee](rdd)
    saver.avro(path).spark.file.run(blocker).unsafeRunSync()
    val t = loaders.avro[Bee](path).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection raw read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/raw.avro"
    delete(path)
    val saver = new RddFileHoader[IO, Ant](rdd)
    saver.avro(path).raw.file.run(blocker).unsafeRunSync()
    val t = loaders.raw.avro[Ant](path)
    assert(ants.toSet == t.collect().toSet)
  }

  test("collection spark read/write identity (happy failure)") {
    val path = "./data/test/spark/persist/avro/ant/spark"
    delete(path)
  }
}
