package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, SaveJackson}
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.FromRecord
import eu.timepit.refined.auto.*
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
@DoNotDiscover
class JacksonTest extends AnyFunSuite {

  def rooster(path: Url): SaveJackson[Rooster] =
    new RddAvroFileHoarder[Rooster](RoosterData.rdd.repartition(3), Rooster.avroCodec).jackson(path)

  val hdp: Hadoop[IO] = sparkSession.hadoop[IO]

  val fromRecord: FromRecord[Rooster] = FromRecord(Rooster.avroCodec)

  def loadRooster(path: Url): IO[Set[Rooster]] =
    hdp
      .filesIn(path)
      .flatMap(_.flatTraverse(hdp.source(_).jackson(100, Rooster.schema).map(fromRecord.from).compile.toList))
      .map(_.toSet)

  val root = "./data/test/spark/persist/jackson/"
  test("1.datetime read/write identity - uncompressed") {
    val path = root / "rooster" / "uncompressed"
    rooster(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()
    val r = sparkSession.loadRdd[Rooster](path).jackson(Rooster.avroCodec)
    assert(RoosterData.expected == r.collect().toSet)
    assert(RoosterData.expected == loadRooster(path).unsafeRunSync())
  }

  def bee(path: Url): SaveJackson[Bee] =
    new RddAvroFileHoarder[Bee](BeeData.rdd.repartition(3), Bee.avroCodec).jackson(path)

  test("2.byte-array read/write identity - multi") {
    import cats.implicits.*
    val path = root / "bee" / "uncompressed"
    bee(path).withCompression(_.Uncompressed).run[IO].unsafeRunSync()

    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("3.byte-array read/write identity - multi.gzip") {
    import cats.implicits.*
    val path = root / "bee" / "gzip"
    bee(path).withCompression(_.Gzip).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("4.byte-array read/write identity - multi.bzip2") {
    import cats.implicits.*
    val path = root / "bee" / "bzip2"
    bee(path).withCompression(_.Bzip2).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("5.byte-array read/write identity - multi.deflate 9") {
    import cats.implicits.*
    val path = root / "bee" / "deflate9"
    bee(path).withCompression(_.Deflate(9)).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("6.byte-array read/write identity - multi.lz4") {
    import cats.implicits.*
    val path = root / "bee" / "lz4"
    bee(path).withCompression(_.Lz4).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("7.byte-array read/write identity - multi.snappy") {
    import cats.implicits.*
    val path = root / "bee" / "snappy"
    bee(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Bee](path).jackson.collect().toList
    assert(BeeData.bees.sortBy(_.b).zip(t.sortBy(_.b)).forall { case (a, b) => a.eqv(b) })
  }

  test("8.jackson jacket") {
    val path = "./data/test/spark/persist/jackson/jacket.json"
    val saver =
      new RddAvroFileHoarder[Jacket](JacketData.rdd.repartition(3), Jacket.avroCodec).jackson(path)
    saver.run[IO].unsafeRunSync()
    val t = sparkSession.loadRdd[Jacket](path).jackson
    assert(JacketData.expected.toSet == t.collect().toSet)
  }

}
