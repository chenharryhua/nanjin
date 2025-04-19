package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.{toHadoopPath, Hadoop}
import eu.timepit.refined.auto.*
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.*
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroTest extends AnyFunSuite {
  val hadoop: Hadoop[IO] = sparkSession.hadoop[IO]

  def singleAvro(path: Url): Set[Rooster] =
    hadoop.source(path).avro(10).map(Rooster.avroCodec.decode).compile.toList.unsafeRunSync().toSet

  def rooster: RddAvroFileHoarder[Rooster] =
    new RddAvroFileHoarder[Rooster](RoosterData.ds.rdd.repartition(3), Rooster.avroCodec)

  def loadRoosters(path: Url): IO[List[Rooster]] =
    hadoop
      .filesIn(path)
      .flatMap(_.flatTraverse(hadoop.source(_).avro(100).map(Rooster.avroCodec.decode).compile.toList))

  val root: Url = Url.parse("./data/test/spark/persist/avro/")

  test("spark agree apache on avro") {
    val path = root / "rooster" / "spark"
    hadoop.delete(path).unsafeRunSync()
    RoosterData.ds.write
      .option("avroSchema", Rooster.schema.toString())
      .format("avro")
      .save(toHadoopPath(path).toString)
    val r  = sparkSession.loadRdd[Rooster](path).avro(Rooster.avroCodec).collect().toSet
    val r2 = loaders.avro(path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == r2)
  }

  test("datetime read/write identity - multi.uncompressed") {
    val path = root / "rooster" / "uncompressed"
    rooster.avro(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.snappy") {
    val path = root / "rooster" / "snappy"
    rooster.avro(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.deflate") {
    val path = root / "rooster" / "deflate"
    rooster.avro(path).withCompression(_.Deflate(3)).run[IO].unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.xz 3") {
    val path = root / "rooster" / "xz3"
    rooster.avro(path).withCompression(_.Xz(3)).run[IO].unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.bzip2") {
    val path = root / "rooster" / "bzip2"
    rooster.avro(path).withCompression(_.Bzip2).run[IO].unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime spark write/nanjin read identity") {
    val path: Url = "./data/test/spark/persist/avro/rooster/spark-write.avro"
    val tds       = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("avroSchema", Rooster.schema.toString)
      .format("avro")
      .save(toHadoopPath(path).toString)
    val r = loaders.rdd.avro[Rooster](path, sparkSession, Rooster.avroCodec).collect().toSet
    val t = loaders.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime spark write without schema/nanjin read identity") {
    val path = "./data/test/spark/persist/avro/rooster/spark-write-no-schema.avro"
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds.repartition(1).write.mode(SaveMode.Overwrite).format("avro").save(path)
    val t = loaders.spark.avro[Rooster](path, sparkSession, Rooster.ate).collect().toSet
    assert(RoosterData.expected == t)
  }

  def bee = new RddAvroFileHoarder[Bee](BeeData.rdd, Bee.avroCodec)
  test("byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = "./data/test/spark/persist/avro/bee/multi.raw"
    bee.avro(path).run[IO].unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, sparkSession, Bee.avroCodec).collect().toList
    val r = loaders.avro[Bee](path, sparkSession, Bee.ate).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /** the data saved to disk is correct. loaders.rdd.avro can not rightly read it back. loaders.avro can.
    * avro4s
    * decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */

  def ant = new RddAvroFileHoarder[Ant](AntData.rdd, Ant.avroCodec)

  test("collection read/write identity multi") {
    import AntData.*
    val path = "./data/test/spark/persist/avro/ant/multi.avro"
    ant.avro(path).run[IO].unsafeRunSync()
    val t = loaders.avro[Ant](path, sparkSession, Ant.ate).collect().toSet
    val r = loaders.rdd.avro[Ant](path, sparkSession, Ant.avroCodec).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("enum read/write identity multi") {
    import CopData.*
    val path  = "./data/test/spark/persist/avro/emcop/raw"
    val saver = new RddAvroFileHoarder[EmCop](emRDD, EmCop.avroCodec).avro(path)
    saver.run[IO].unsafeRunSync()
    val t = loaders.avro[EmCop](path, sparkSession, EmCop.ate).collect().toSet
    val r = loaders.rdd.avro[EmCop](path, sparkSession, EmCop.avroCodec).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

//  test("sealed trait read/write identity multi/raw") {
//    import CopData.*
//    val path  = NJPath("./data/test/spark/persist/avro/cocop/multi.avro")
//    val saver = new RddAvroFileHoarder[IO, CoCop](IO(coRDD), CoCop.avroCodec).avro(path)
//    saver.run.unsafeRunSync()
//    val t = loaders.avro[CoCop](path, sparkSession, CoCop.ate).collect().toSet
//    assert(coCops.toSet == t)
//  }

  test("coproduct read/write identity - rdd") {
    import CopData.*
    val path  = "./data/test/spark/persist/avro/cpcop/multi.avro"
    val saver = new RddAvroFileHoarder[CpCop](cpRDD, CpCop.avroCodec).avro(path)
    saver.run[IO].unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, sparkSession, CpCop.avroCodec).collect().toSet
    assert(cpCops.toSet == t)
  }

//  test("coproduct read/write identity - dataset - happy failure") {
//    import CopData.*
//    val path  = NJPath("./data/test/spark/persist/avro/cpcop/ds/multi.avro")
//    val saver = new RddAvroFileHoarder[IO, CpCop](IO(cpRDD), CpCop.avroCodec).avro(path)
//    saver.run.unsafeRunSync()
//    val t = loaders.rdd.avro[CpCop](path, sparkSession, CpCop.avroCodec).collect().toSet
//    assert(cpCops.toSet == t)
//    intercept[Throwable](loaders.avro[CpCop](path, sparkSession, CpCop.ate).collect().toSet)
//  }
}
