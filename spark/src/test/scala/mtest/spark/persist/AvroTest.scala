package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class AvroTest extends AnyFunSuite {

  test("datetime rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/raw"
    delete(path)
    savers.raw.avro(rdd, path)
    val r: RDD[Rooster]          = loaders.raw.avro[Rooster](path)
    val t: TypedDataset[Rooster] = loaders.avro[Rooster](path)
    assert(expected == r.collect().toSet)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("datetime spark read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/avro/rooster/spark"
    delete(path)
    savers.avro(rdd, path)
    val t: TypedDataset[Rooster] = loaders.avro[Rooster](path)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("byte-array rdd read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/raw"
    delete(path)
    savers.raw.avro(rdd, path)
    val t = loaders.raw.avro[Bee](path)
    assert(bees.sortBy(_.b).zip(t.collect().toList.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array spark read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/spark"
    delete(path)
    savers.avro(rdd, path)
    val t = loaders.avro[Bee](path).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection raw read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/raw"
    delete(path)
    savers.raw.avro(rdd, path)
    val t = loaders.raw.avro[Ant](path)
    assert(ants.toSet == t.collect().toSet)
  }

  test("collection spark read/write identity (happy failure)") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/spark"
    delete(path)
    // assertThrows[Throwable](savers.avro(rdd, path))
  }
}
