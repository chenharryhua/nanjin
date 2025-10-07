package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.{Compression, Hadoop}
import com.sksamuel.avro4s.FromRecord
import eu.timepit.refined.auto.*
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ParquetTest extends AnyFunSuite {
  import RoosterData.*
  val hdp: Hadoop[IO] = sparkSession.hadoop[IO]

  def loadRooster(path: Url): IO[Set[Rooster]] =
    hdp
      .filesIn(path)
      .flatMap(
        _.flatTraverse(hdp.source(_).parquet(100).map(FromRecord(Rooster.avroCodec).from).compile.toList))
      .map(_.toSet)

  def roosterSaver(path: Url): SaveParquet[Rooster] =
    new RddAvroFileHoarder[Rooster](RoosterData.rdd, Rooster.avroCodec).parquet(path)

  val root = "./data/test/spark/persist/parquet"

  test("2.apache parquet =!= spark parquet") {
    val path = root / "rooster" / "spark2"
    roosterSaver(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    // intercept[Throwable](sparkSession.read.parquet(path.pathStr).show())
  }

  test("3.datetime read/write identity multi.uncompressed") {
    val path = root / "rooster" / "uncompressed"
    roosterSaver(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("4.datetime read/write identity multi.snappy") {
    val path = root / "rooster" / "snappy"
    roosterSaver(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("5.datetime read/write identity multi.lz4") {
    val path = root / "rooster" / "lz4"
    roosterSaver(path).withCompression(_.Lz4).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("6.datetime read/write identity multi.lz4raw") {
    val path = root / "rooster" / "lz4raw"
    roosterSaver(path).withCompression(_.Lz4_Raw).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
    val t = loadRooster(path)
    assert(expected == r)
    assert(expected == t.unsafeRunSync())
  }

  test("7.datetime read/write identity multi.zstd") {
    val path = root / "rooster" / "zstd"
    val compression = Compression.Zstandard(1)
    roosterSaver(path).withCompression(compression).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
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
    saveRDD.parquet(RoosterData.rdd, path, Rooster.avroCodec, Compression.Gzip)
    val r = sparkSession.loadRdd[Rooster](path).parquet(Rooster.avroCodec).collect().toSet
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

  def beeSaver(path: Url): SaveParquet[Bee] =
    new RddAvroFileHoarder[Bee](BeeData.rdd, Bee.avroEncoder).parquet(path)

  test("10.byte-array read/write identity mulit uncompress") {
    import BeeData.*
    import cats.implicits.*
    val path = "./data/test/spark/persist/parquet/bee/multi.parquet"
    beeSaver(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).parquet.collect().toList
    assert(bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("11.collection read/write identity multi uncompress") {
    import AntData.*
    val path = "./data/test/spark/persist/parquet/ant/multi.parquet"
    val saver = new RddAvroFileHoarder[Ant](rdd, Ant.avroEncoder).parquet(path)
    saver.withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Ant](path).parquet(Ant.avroCodec).collect().toSet
    assert(ants.toSet == t)
  }

  test("12.enum cop read/write identity") {
    import CopData.*
    val path = "./data/test/spark/persist/parquet/emcop/multi.parquet"
    val saver = new RddAvroFileHoarder[EmCop](emRDD, EmCop.avroCodec).parquet(path)
    saver.withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[EmCop](path).parquet(EmCop.avroCodec).collect().toSet
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
}
