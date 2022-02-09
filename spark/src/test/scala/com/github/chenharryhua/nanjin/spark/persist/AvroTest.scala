package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroTest extends AnyFunSuite {
  val hadoop: NJHadoop[IO] = sparkSession.hadoop[IO]

  def singleAvro(path: NJPath): Set[Rooster] =
    hadoop
      .avro(Rooster.avroCodec.schema)
      .source(path)
      .map(Rooster.avroCodec.avroDecoder.decode)
      .compile
      .toList
      .unsafeRunSync()
      .toSet

  def rooster(path: NJPath) =
    new RddAvroFileHoarder[IO, Rooster](
      RoosterData.ds.rdd.repartition(3),
      Rooster.avroCodec.avroEncoder,
      HoarderConfig(path))

  def loadRoosters(path: NJPath) = {
    val rst =
      hadoop
        .inputFilesByName(path)
        .map(_.foldLeft(fs2.Stream.empty.covaryAll[IO, Rooster]) { case (ss, p) =>
          ss ++ hadoop.avro(Rooster.schema).source(p).map(Rooster.avroCodec.avroDecoder.decode)
        })

    fs2.Stream.force(rst).compile.toList
  }

  test("datetime read/write identity - multi.uncompressed") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.uncompressed.avro")
    rooster(path).avro.uncompress.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.snappy") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.snappy.avro")
    rooster(path).avro.snappy.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.deflate") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.deflate.avro")
    rooster(path).avro.deflate(3).run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.xz") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.xz.avro")
    rooster(path).avro.xz(3).run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime read/write identity - multi.bzip2") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/multi.bzip2.avro")
    rooster(path).avro.bzip2.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime spark write/nanjin read identity") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/spark-write.avro")
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("avroSchema", Rooster.schema.toString)
      .format("avro")
      .save(path.pathStr)
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == loadRoosters(path).unsafeRunSync().toSet)
  }

  test("datetime spark write without schema/nanjin read identity") {
    val path = NJPath("./data/test/spark/persist/avro/rooster/spark-write-no-schema.avro")
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds.repartition(1).write.mode(SaveMode.Overwrite).format("avro").save(path.pathStr)
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == t)
  }

  def bee(path: NJPath) = new RddAvroFileHoarder[IO, Bee](BeeData.rdd, Bee.avroCodec.avroEncoder, HoarderConfig(path))
  test("byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = NJPath("./data/test/spark/persist/avro/bee/multi.raw")
    bee(path).avro.run.unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    val r = loaders.avro[Bee](path, Bee.ate, sparkSession).collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /** the data saved to disk is correct. loaders.rdd.avro can not rightly read it back. loaders.avro can. avro4s
    * decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */

  def ant(path: NJPath) = new RddAvroFileHoarder[IO, Ant](AntData.rdd, Ant.avroCodec.avroEncoder, HoarderConfig(path))

  test("collection read/write identity multi") {
    import AntData.*
    val path = NJPath("./data/test/spark/persist/avro/ant/multi.avro")
    ant(path).avro.run.unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate, sparkSession).collect().toSet
    val r = loaders.rdd.avro[Ant](path, Ant.avroCodec.avroDecoder, sparkSession).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("enum read/write identity multi") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/emcop/raw")
    val saver = new RddAvroFileHoarder[IO, EmCop](emRDD, EmCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.run.unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate, sparkSession).collect().toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("sealed trait read/write identity multi/raw (happy failure)") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cocop/multi.avro")
    val saver = new RddAvroFileHoarder[IO, CoCop](coRDD, CoCop.avroCodec.avroEncoder, HoarderConfig(path))
    intercept[Throwable](saver.avro.run.unsafeRunSync())
    //  val t = loaders.raw.avro[CoCop](path).collect().toSet
    //  assert(coCops.toSet == t)
  }

  test("coproduct read/write identity - multi") {
    import CopData.*
    val path  = NJPath("./data/test/spark/persist/avro/cpcop/multi.avro")
    val saver = new RddAvroFileHoarder[IO, CpCop](cpRDD, CpCop.avroCodec.avroEncoder, HoarderConfig(path))
    saver.avro.run.unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(cpCops.toSet == t)
  }
}
