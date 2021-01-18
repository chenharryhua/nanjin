package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetAvroFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import mtest.spark._
import org.scalatest.funsuite.AnyFunSuite

//@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData._

  test("datetime read/write identity multi.uncompressed") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.uncompressed.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).errorIfExists.ignoreIfExists.overwrite.outPath(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(expected == r)
  }

  ignore("datetime read/write identity single.uncompressed") {
    val path  = "./data/test/spark/persist/parquet/rooster/single/uncompressed.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).file.outPath(path).run(blocker).unsafeRunSync()
    sparkSession.read.parquet(path).show()
    // val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    // assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.snappy.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).snappy.folder.run(blocker).unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(expected == r)
  }

  ignore("datetime read/write identity single.snappy") {
    val path  = "./data/test/spark/persist/parquet/rooster/single.snappy.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).snappy.file.run(blocker).unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.gzip") {
    val path  = "./data/test/spark/persist/parquet/rooster/multi.gzip.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).gzip.run(blocker).unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(expected == r)
  }

  test("byte-array read/write identity") {
    import BeeData._
    import cats.implicits._
    val path  = "./data/test/spark/persist/parquet/bee/multi.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Bee](ds, Bee.avroEncoder)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate, sparkSession).collect[IO].unsafeRunSync().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity") {
    import AntData._
    val path  = "./data/test/spark/persist/parquet/ant/multi.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Ant](ds, Ant.avroEncoder)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate, sparkSession).dataset.collect.toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity") {
    import CopData._
    val path  = "./data/test/spark/persist/parquet/emcop/multi.parquet"
    val saver = new DatasetAvroFileHoarder[IO, EmCop](emDS, EmCop.avroCodec.avroEncoder)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate, sparkSession).dataset.collect.toSet
    assert(emCops.toSet == t)
  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("parquet jacket multi") {
    import JacketData._
    val path  = "./data/test/spark/persist/parquet/jacket/multi/jacket.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("parquet jacket single uncompressed") {
    import JacketData._
    val path  = "./data/test/spark/persist/parquet/jacket/single/jacket.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).file.run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("parquet jacket single snappy") {
    import JacketData._
    val path  = "./data/test/spark/persist/parquet/jacket/single/jacket.snappy.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).file.snappy.run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }

  test("parquet jacket single gzip") {
    import JacketData._
    val path  = "./data/test/spark/persist/parquet/jacket/single/jacket.gz.parquet"
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).file.gzip.run(blocker).unsafeRunSync()
    val t: TypedDataset[Jacket] = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect[IO]().unsafeRunSync().toSet)
  }
}
