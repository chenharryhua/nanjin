package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder}
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark.*
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.GenericRecordCodec

@DoNotDiscover
class AvroTest extends AnyFunSuite {
  val hadoop: NJHadoop[IO]                = NJHadoop[IO](sparkSession.sparkContext.hadoopConfiguration)
  val gr: GenericRecordCodec[IO, Rooster] = new GenericRecordCodec[IO, Rooster]()

  def singleAvro(path: String): Set[Rooster] = hadoop
    .avroSource(path, Rooster.avroCodec.schema)
    .through(gr.decode(Rooster.avroCodec.avroDecoder))
    .compile
    .toList
    .unsafeRunSync()
    .toSet

  val rooster =
    new RddAvroFileHoarder[IO, Rooster](RoosterData.ds.rdd.repartition(3), Rooster.avroCodec.avroEncoder)

  test("datetime read/write identity - multi.uncompressed") {
    val path = "./data/test/spark/persist/avro/rooster/multi.uncompressed.avro"
    rooster.avro(path).uncompress.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.snappy") {
    val path = "./data/test/spark/persist/avro/rooster/multi.snappy.avro"
    rooster.avro(path).snappy.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.deflate") {
    val path = "./data/test/spark/persist/avro/rooster/multi.deflate.avro"
    rooster.avro(path).deflate(3).folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.xz") {
    val path = "./data/test/spark/persist/avro/rooster/multi.xz.avro"
    rooster.avro(path).xz(3).folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - multi.bzip2") {
    val path = "./data/test/spark/persist/avro/rooster/multi.bzip2.avro"
    rooster.avro(path).bzip2.folder.run.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime read/write identity - single.uncompressed") {
    val path = "./data/test/spark/persist/avro/rooster/single.uncompressed.avro"
    rooster.avro(path).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))

    val t3 = loaders.stream
      .avro[IO, Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession.sparkContext.hadoopConfiguration)
      .compile
      .toList
      .unsafeRunSync()
      .toSet

    assert(RoosterData.expected == t3)
  }

  test("datetime read/write identity - single.snappy") {
    val path = "./data/test/spark/persist/avro/rooster/single.snappy.avro"
    rooster.avro(path).snappy.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.bzip2") {
    val path = "./data/test/spark/persist/avro/rooster/single.bzip2.avro"
    rooster.avro(path).bzip2.file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.deflate") {
    val path = "./data/test/spark/persist/avro/rooster/single.deflate.avro"
    rooster.avro(path).deflate(5).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime read/write identity - single.xz") {
    val path = "./data/test/spark/persist/avro/rooster/single.xz.avro"
    rooster.avro(path).xz(5).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    assert(RoosterData.expected == singleAvro(path))
  }

  test("datetime spark write/nanjin read identity") {
    val path = "./data/test/spark/persist/avro/rooster/spark-write.avro"
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("avroSchema", Rooster.schema.toString)
      .format("avro")
      .save(path)
    val r = loaders.rdd.avro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("datetime spark write without schema/nanjin read identity") {
    val path = "./data/test/spark/persist/avro/rooster/spark-write-no-schema.avro"
    val tds  = Rooster.ate.normalize(RoosterData.rdd, sparkSession)
    tds.repartition(1).write.mode(SaveMode.Overwrite).format("avro").save(path)
    val t = loaders.avro[Rooster](path, Rooster.ate, sparkSession).dataset.collect.toSet
    assert(RoosterData.expected == t)
  }

  val bee = new RddAvroFileHoarder[IO, Bee](BeeData.rdd, Bee.avroCodec.avroEncoder)
  test("byte-array read/write identity - multi") {
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/multi.raw"
    bee.avro(path).folder.run.unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    val r = loaders.avro[Bee](path, Bee.ate, sparkSession).dataset.collect.toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("byte-array read/write identity single") {
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/single.raw.avro"
    bee.avro(path).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.avro[Bee](path, Bee.ate, sparkSession).dataset.collect.toList
    assert(BeeData.bees.sortBy(_.b).zip(r.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  /** the data saved to disk is correct. loaders.rdd.avro can not rightly read it back. loaders.avro can. avro4s
    * decode:https://github.com/sksamuel/avro4s/blob/release/4.0.x/avro4s-core/src/main/scala/com/sksamuel/avro4s/ByteIterables.scala#L31
    * should follow spark's implementation:
    * https://github.com/apache/spark/blob/branch-3.0/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDeserializer.scala#L158
    */

  test("byte-array read/write identity single - use customized codec") {
    import cats.implicits._
    val path = "./data/test/spark/persist/avro/bee/single2.raw.avro"
    bee.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).collect().toList
    println(loaders.rdd.avro[Bee](path, Bee.avroCodec.avroDecoder, sparkSession).map(_.toWasp).collect().toList)
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  val ant = new RddAvroFileHoarder[IO, Ant](AntData.rdd, Ant.avroCodec.avroEncoder)
  test("collection read/write identity single") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/single.raw.avro"
    ant.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Ant](path, Ant.avroCodec.avroDecoder, sparkSession).collect().toSet
    val r = loaders.avro[Ant](path, Ant.ate, sparkSession).dataset.collect.toSet
    assert(ants.toSet == t)
    assert(ants.toSet == r)
  }

  test("collection read/write identity multi") {
    import AntData._
    val path = "./data/test/spark/persist/avro/ant/multi.avro"
    ant.avro(path).folder.run.unsafeRunSync()
    val t = loaders.avro[Ant](path, Ant.ate, sparkSession).dataset.collect.toSet
    val r = loaders.rdd.avro[Ant](path, Ant.avroCodec.avroDecoder, sparkSession).collect().toSet

    assert(ants.toSet == t)
    assert(ants.toSet == t)
  }

  test("enum read/write identity single") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/emcop/single.avro"
    val saver = new RddAvroFileHoarder[IO, EmCop](emRDD, EmCop.avroCodec.avroEncoder)
    saver.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate, sparkSession).dataset.collect.toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("enum read/write identity multi") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/emcop/raw"
    val saver = new RddAvroFileHoarder[IO, EmCop](emRDD, EmCop.avroCodec.avroEncoder)
    saver.avro(path).folder.run.unsafeRunSync()
    val t = loaders.avro[EmCop](path, EmCop.ate, sparkSession).dataset.collect.toSet
    val r = loaders.rdd.avro[EmCop](path, EmCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(emCops.toSet == t)
    assert(emCops.toSet == r)
  }

  test("sealed trait read/write identity single/raw (happy failure)") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cocop/single.avro"
    val saver = new RddAvroFileHoarder[IO, CoCop](coRDD, CoCop.avroCodec.avroEncoder)
    saver.avro(path).file.sink.compile.drain.unsafeRunSync()
    intercept[Throwable](loaders.rdd.avro[CoCop](path, CoCop.avroCodec.avroDecoder, sparkSession).collect().toSet)
    // assert(coCops.toSet == t)
  }

  test("sealed trait read/write identity multi/raw (happy failure)") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cocop/multi.avro"
    val saver = new RddAvroFileHoarder[IO, CoCop](coRDD, CoCop.avroCodec.avroEncoder)
    intercept[Throwable](saver.avro(path).folder.run.unsafeRunSync())
    //  val t = loaders.raw.avro[CoCop](path).collect().toSet
    //  assert(coCops.toSet == t)
  }

  test("coproduct read/write identity - multi") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cpcop/multi.avro"
    val saver = new RddAvroFileHoarder[IO, CpCop](cpRDD, CpCop.avroCodec.avroEncoder)
    saver.avro(path).folder.run.unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("coproduct read/write identity - single") {
    import CopData._
    val path  = "./data/test/spark/persist/avro/cpcop/single.avro"
    val saver = new RddAvroFileHoarder[IO, CpCop](cpRDD, CpCop.avroCodec.avroEncoder)
    saver.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[CpCop](path, CpCop.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(cpCops.toSet == t)
  }

  test("avro jacket") {
    import JacketData._
    val path  = "./data/test/spark/persist/avro/jacket.avro"
    val saver = new RddAvroFileHoarder[IO, Jacket](rdd, Jacket.avroCodec.avroEncoder)
    saver.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t = loaders.rdd.avro[Jacket](path, Jacket.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(expected.toSet == t)
    val t2 = loaders.avro[Jacket](path, Jacket.ate, sparkSession).dataset.collect.toSet
    assert(expected.toSet == t2)
  }

  test("avro fractual") {
    import FractualData._
    val path  = "./data/test/spark/persist/avro/fractual.avro"
    val saver = new RddAvroFileHoarder[IO, Fractual](rdd, Fractual.avroCodec.avroEncoder)
    saver.avro(path).file.sink.compile.drain.unsafeRunSync()
    val t =
      loaders.rdd.avro[Fractual](path, Fractual.avroCodec.avroDecoder, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
