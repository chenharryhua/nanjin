package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.DatasetFileHoarder

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData._

  test("datetime read/write identity multi.uncompressed") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.uncompressed.parquet"
    val saver = new DatasetFileHoarder[IO, Rooster](ds)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.snappy.parquet"
    val saver = new DatasetFileHoarder[IO, Rooster](ds)
    saver.parquet(path).snappy.run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.gzip") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.gzip.parquet"
    val saver = new DatasetFileHoarder[IO, Rooster](ds)
    saver.parquet(path).gzip.run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
  }

  test("byte-array read/write identity") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/parquet/bee/multi.parquet"
    val saver = new DatasetFileHoarder[IO, Bee](ds)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity") {
    import AntData._
    val path  = "./data/test/spark/persist/parquet/ant/multi.parquet"
    val saver = new DatasetFileHoarder[IO, Ant](ds)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate).collect[IO]().unsafeRunSync().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity") {
    import CopData._
    val path  = "./data/test/spark/persist/parquet/emcop/multi.parquet"
    val saver = new DatasetFileHoarder[IO, EmCop](emDS)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate).collect[IO].unsafeRunSync().toSet
    assert(emCops.toSet == t)
  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("parquet jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/parquet/jacket.parquet"
    val saver = new DatasetFileHoarder[IO, Jacket](ds)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.parquet(path, Jacket.ate)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
