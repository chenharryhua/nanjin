package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData._

  test("datetime read/write identity") {
    val path = "./data/test/spark/persist/parquet/rooster/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd).repartition(1)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("byte-array read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/parquet/bee/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd).repartition(1)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Bee](path).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/parquet/ant/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd).repartition(1)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Ant](path).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity") {
    import CopData._
    val path = "./data/test/spark/persist/parquet/emcop/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, EmCop](emRDD).repartition(1)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[EmCop](path).collect[IO].unsafeRunSync().toSet
    assert(emCops.toSet == t)
  }

  /**
    * frameless/spark does not support coproduct so cocop and cpcop do not compile
    */
}
