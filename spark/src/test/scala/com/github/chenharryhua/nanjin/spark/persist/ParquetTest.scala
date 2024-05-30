package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.{HadoopParquet, NJCompression, NJHadoop, NJPath}
import com.sksamuel.avro4s.FromRecord
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData.*
  val hdp: NJHadoop[IO] = sparkSession.hadoop[IO]

  val parquet: HadoopParquet[IO] = hdp.parquet(Rooster.avroCodec.schema)

  def loadRooster(path: NJPath): IO[Set[Rooster]] =
    fs2.Stream
      .eval(hdp.filesIn(path))
      .flatMap(parquet.source(_, 10))
      .map(FromRecord(Rooster.avroCodec).from)
      .compile
      .toList
      .map(_.toSet)

  def roosterSaver(path: NJPath): SaveParquet[IO, Rooster] =
    new RddAvroFileHoarder[IO, Rooster](IO(RoosterData.rdd), Rooster.avroCodec).parquet(path)

  val root: NJPath = NJPath("./data/test/spark/persist/parquet")

  test("1.spark parquet =!= apache parquet") {
    val path = root / "rooster" / "spark"
    hdp.delete(path).unsafeRunSync()
    ds.write.parquet(path.pathStr)
    val r = loaders.spark.parquet(path, sparkSession, Rooster.ate).collect().toSet
    assert(expected == r)
    // intercept[Throwable](loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec.avroDecoder).collect())
  }

  test("2.apache parquet =!= spark parquet") {
    val path = root / "rooster" / "spark2"
    roosterSaver(path).withCompression(_.Uncompressed).run.unsafeRunSync()
    // intercept[Throwable](sparkSession.read.parquet(path.pathStr).show())
  }

  test("3.datetime read/write identity multi.uncompressed") {
    val path = root / "rooster" / "uncompressed"
    roosterSaver(path).withCompression(_.Uncompressed).run.unsafeRunSync()
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("4.datetime read/write identity multi.snappy") {
    val path = root / "rooster" / "snappy"
    roosterSaver(path).withCompression(_.Snappy).run.unsafeRunSync()
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("5.datetime read/write identity multi.lz4") {
    val path = root / "rooster" / "lz4"
    roosterSaver(path).withCompression(_.Lz4).run.unsafeRunSync()
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("6.datetime read/write identity multi.lz4raw") {
    val path = root / "rooster" / "lz4raw"
    roosterSaver(path).withCompression(_.Lz4_Raw).run.unsafeRunSync()
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("7.datetime read/write identity multi.zstd") {
    val path        = root / "rooster" / "zstd"
    val compression = NJCompression.Zstandard(1)
    roosterSaver(path).withCompression(compression).run.unsafeRunSync()
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

//  test("8.datetime read/write identity multi.lzo") {
//    val path = root / "rooster" / "lzo"
//    hdp.delete(path).unsafeRunSync()
//    saveRDD.parquet(RoosterData.rdd, path, Rooster.avroCodec.avroEncoder, NJCompression.Lzo)
//    val r = loaders.rdd.parquet(path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
//    val t = loadRooster(path)
//    assert(expected == r)
//    assert(expected == t.unsafeRunSync())
//  }

  test("9.datetime read/write identity multi.gzip") {
    val path = root / "rooster" / "gzip"
    hdp.delete(path).unsafeRunSync()
    saveRDD.parquet(RoosterData.rdd, path, Rooster.avroCodec, NJCompression.Gzip)
    val r = loaders.rdd.parquet(path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

//  test("datetime read/write identity multi.brotli") {
//    val path = root / "rooster" / "brotli"
//    roosterSaver(path).brotli.run.unsafeRunSync()
//    val r =
//      loaders.parquet[Rooster](path, Rooster.ate, sparkSession).collect().toSet
//    assert(expected == r)
//  }

  def beeSaver(path: NJPath): SaveParquet[IO, Bee] =
    new RddAvroFileHoarder[IO, Bee](IO(BeeData.rdd), Bee.avroEncoder).parquet(path)

  test("10.byte-array read/write identity mulit uncompress") {
    import BeeData.*
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/parquet/bee/multi.parquet")
    beeSaver(path).withCompression(_.Uncompressed).run.unsafeRunSync()
    val t = loaders.parquet[Bee](path, sparkSession, Bee.ate).collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("11.collection read/write identity multi uncompress") {
    import AntData.*
    val path  = NJPath("./data/test/spark/persist/parquet/ant/multi.parquet")
    val saver = new RddAvroFileHoarder[IO, Ant](IO(rdd), Ant.avroEncoder).parquet(path)
    saver.withCompression(_.Uncompressed).run.unsafeRunSync()
    val t = loaders.parquet[Ant](path, sparkSession, Ant.ate).collect().toSet
    assert(ants.toSet == t)
  }

  test("12.enum cop read/write identity") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/parquet/emcop/multi.parquet")
    val saver = new RddAvroFileHoarder[IO, EmCop](IO(emRDD), EmCop.avroCodec).parquet(path)
    saver.withCompression(_.Uncompressed).run.unsafeRunSync()
    val t = loaders.parquet[EmCop](path, sparkSession, EmCop.ate).collect().toSet
    assert(emCops.toSet == t)
  }

//  test("coproduct cop read/write identity - happy failure") {
//    import CopData.*
//    val path  = NJPath("./data/test/spark/persist/parquet/cpcop/multi.parquet")
//    val saver = new RddAvroFileHoarder[IO, CpCop](IO(cpRDD), CpCop.avroCodec.avroEncoder).parquet(path)
//    // intercept[Throwable](saver.uncompress.run.unsafeRunSync())
//    // assert(cpCops.toSet == t)
//  }

//  test("case object cop read/write identity - happy failure") {
//    import CopData.*
//    val path  = NJPath("./data/test/spark/persist/parquet/cocop/multi.parquet")
//    val saver = new RddAvroFileHoarder[IO, CoCop](IO(coRDD), CoCop.avroCodec.avroEncoder).parquet(path)
//    // intercept[Throwable](saver.uncompress.run.unsafeRunSync())
//    // assert(coCops.toSet == t)
//  }

  /** frameless/spark does not support coproduct so cocop and cpcop do not compile
    */

  test("13.parquet jacket multi uncompress") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/parquet/jacket/multi/jacket.parquet")
    val saver = new RddAvroFileHoarder[IO, Jacket](IO(rdd), Jacket.avroCodec).parquet(path)
    saver.withCompression(_.Uncompressed).run.unsafeRunSync()
    val t = loaders.parquet(path, sparkSession, Jacket.ate)
    assert(expected.toSet == t.collect().toSet)
  }
}
