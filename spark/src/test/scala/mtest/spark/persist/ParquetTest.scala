package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData._

  test("datetime read/write identity multi.uncompressed") {
    val path = "./data/test/spark/persist/parquet/rooster/multi.uncompressed.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec).repartition(1)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path = "./data/test/spark/persist/parquet/rooster/multi.snappy.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec).repartition(2)
    saver.parquet(path).snappy.run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.gzip") {
    val path = "./data/test/spark/persist/parquet/rooster/multi.gzip.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd, Rooster.avroCodec).repartition(3)
    saver.parquet(path).gzip.run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("byte-array read/write identity") {
    import BeeData._
    import cats.implicits._
    val path = "./data/test/spark/persist/parquet/bee/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Bee](rdd, Bee.codec).repartition(4)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity") {
    import AntData._
    val path = "./data/test/spark/persist/parquet/ant/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, Ant](rdd, Ant.codec).repartition(5)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity") {
    import CopData._
    val path = "./data/test/spark/persist/parquet/emcop/multi.parquet"
    delete(path)
    val saver = new RddFileHoarder[IO, EmCop](emRDD, EmCop.codec).repartition(6)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate).collect[IO].unsafeRunSync().toSet
    assert(emCops.toSet == t)
  }

  /**
    * frameless/spark does not support coproduct so cocop and cpcop do not compile
    */
}
