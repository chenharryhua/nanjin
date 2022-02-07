package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData.*

  test("datetime read/write identity multi.uncompressed") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.uncompressed.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.snappy.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.snappy.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.lz4") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.lz4.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.lz4.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.zstd") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.zstd.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.zstd(5).run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.gzip") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.gzip.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.gzip.run.unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("byte-array read/write identity mulit uncompress") {
    import BeeData.*
    import cats.implicits.*
    val path  = NJPath("./data/test/spark/persist/parquet/bee/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Bee](ds, Bee.avroEncoder, HoarderConfig(path))
    saver.parquet.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity multi uncompress") {
    import AntData.*
    val path  = NJPath("./data/test/spark/persist/parquet/ant/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Ant](ds, Ant.avroEncoder, HoarderConfig(path))
    saver.parquet.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate, sparkSession).collect().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity multi uncompress") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/parquet/emcop/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, EmCop](emDS, EmCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    assert(emCops.toSet == t)
  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("parquet jacket multi uncompress") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/multi/jacket.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder, HoarderConfig(path))
    saver.parquet.uncompress.run.unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }
}
