package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData.*
  val hdp = sparkSession.hadoop[IO]

  def loadRooster(path: NJPath) =
    fs2.Stream
      .force(
        hdp
          .filesSortByName(path)
          .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Rooster]) { case (ss, p) =>
            ss ++ hdp.parquet(Rooster.schema).source(p).map(Rooster.avroCodec.fromRecord)
          }))
      .compile
      .toList
      .map(_.toSet)

  def roosterSaver(path: NJPath) =
    new DatasetAvroFileHoarder[IO, Rooster](RoosterData.ds, Rooster.avroCodec.avroEncoder).parquet(path)

  val root = NJPath("./data/test/spark/persist/parquet")
  test("datetime read/write identity multi.uncompressed") {
    val path = root / "rooster" / "uncompressed"
    roosterSaver(path).append.errorIfExists.ignoreIfExists.overwrite.uncompress.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.snappy") {
    val path = root / "rooster" / "snappy"
    roosterSaver(path).snappy.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.lz4") {
    val path = root / "rooster" / "lz4"
    roosterSaver(path).lz4.run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.zstd") {
    val path = root / "rooster" / "zstd"
    roosterSaver(path).zstd(5).run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

  test("datetime read/write identity multi.zstd-2") {
    val path = root / "rooster" / "zstandard"
    roosterSaver(path).withCompression(CompressionCodecName.ZSTD).run.unsafeRunSync()
    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

//  test("datetime read/write identity multi.lzo") {
//    val path = root / "rooster" / "lzo"
//    roosterSaver(path).lzo.run.unsafeRunSync()
//    val r = loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
//    assert(expected == r)
//  }

  test("datetime read/write identity multi.gzip") {
    val path = root / "rooster" / "gzip"
    roosterSaver(path).gzip.run.unsafeRunSync()
    val r =
      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(expected == r)
  }

//  test("datetime read/write identity multi.brotli") {
//    val path = root / "rooster" / "brotli"
//    roosterSaver(path).brotli.run.unsafeRunSync()
//    val r =
//      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
//    assert(expected == r)
//  }

  def beeSaver(path: NJPath) =
    new DatasetAvroFileHoarder[IO, Bee](BeeData.ds, Bee.avroEncoder).parquet(path)

  test("byte-array read/write identity mulit uncompress") {
    import BeeData.*
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/parquet/bee/multi.parquet")
    beeSaver(path).uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("collection read/write identity multi uncompress") {
    import AntData.*
    val path  = NJPath("./data/test/spark/persist/parquet/ant/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Ant](ds, Ant.avroEncoder).parquet(path)
    saver.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[Ant](path, Ant.ate, sparkSession).collect().toSet
    assert(ants.toSet == t)
  }

  test("enum read/write identity multi uncompress") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/parquet/emcop/multi.parquet")
    val saver = new DatasetAvroFileHoarder[IO, EmCop](emDS, EmCop.avroCodec.avroEncoder).parquet(path)
    saver.uncompress.run.unsafeRunSync()
    val t = loaders.parquet[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    assert(emCops.toSet == t)
  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("parquet jacket multi uncompress") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/multi/jacket.parquet")
    val saver = new DatasetAvroFileHoarder[IO, Jacket](ds, Jacket.avroCodec.avroEncoder).parquet(path)
    saver.uncompress.run.unsafeRunSync()
    val t = loaders.parquet(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }
}
