package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetAvroFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.TypedDataset
import mtest.spark.*
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData.*

  test("datetime read/write identity multi.uncompressed") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.uncompressed.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).folder.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity single.uncompressed - happy failure") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/single/uncompressed.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.UNCOMPRESSED))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    assertThrows[Exception](loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet)
    // assert(expected == r)
  }

  test("datetime read/write identity single.lz4 - happy failure") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/single/lz4.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.LZ4))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    assertThrows[Exception](loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet)
    // assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.snappy.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).folder.snappy.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.lz4") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.lz4.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).folder.lz4.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.zstd") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.zstd.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).folder.zstd(5).run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.gzip") {
    val path  = NJPath("./data/test/spark/persist/parquet/rooster/multi.gzip.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Rooster](ds, Rooster.avroCodec.avroEncoder)
    saver.parquet(path).folder.gzip.run.unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("byte-array read/write identity mulit uncompress") {
    import BeeData.*
    import cats.implicits.*
    val path  = NJPath("./data/test/spark/persist/parquet/bee/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Bee](ds, Bee.avroEncoder)
    saver.parquet(path).folder.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity single gzip") {
    import BeeData.*
    import cats.implicits.*
    val path  = NJPath("./data/test/spark/persist/parquet/bee/single.gz.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Bee](ds, Bee.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.GZIP))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity multi uncompress") {
    import AntData.*
    val path  = NJPath("./data/test/spark/persist/parquet/ant/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Ant](ds, Ant.avroEncoder)
    saver.parquet(path).folder.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate, sparkSession).collect().toSet
    assert(ants.toSet == t)
  }

  test("collection read/write identity single snappy") {
    import AntData.*
    val path  = NJPath("./data/test/spark/persist/parquet/ant/single.snappy.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Ant](ds, Ant.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.SNAPPY))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate, sparkSession).collect().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity multi uncompress") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/parquet/emcop/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, EmCop](emDS, EmCop.avroCodec.avroEncoder)
    saver.parquet(path).folder.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    assert(emCops.toSet == t)
  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("parquet jacket multi uncompress") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/multi/jacket.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).folder.uncompress.run.unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }

  test("parquet jacket single uncompressed") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/single/jacket.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver.parquet(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }

  test("parquet jacket single snappy") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/single/jacket.snappy.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.SNAPPY))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }

  test("parquet jacket single gzip") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/single/jacket.gz.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder)
    saver
      .parquet(path)
      .file
      .updateBuilder(_.withCompressionCodec(CompressionCodecName.GZIP))
      .sink
      .compile
      .drain
      .unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }
}
