package mtest.spark.pipe

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.terminals.{NJHeaderEncoder, NJPath}
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import kantan.csv.{CsvConfiguration, RowDecoder}
import kantan.csv.generic.*
import monocle.syntax.all.*
import mtest.spark.sparkSession
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.*

object ReadWriteTestData {
  final case class TestData(index: Long, name: String)
  val number = 10000
  val cr     = TestData(0, "abc")
  val data =
    Stream.emits(List.fill(number)(cr)).covary[IO].chunkLimit(2).unchunks.zipWithIndex.map { case (cr, idx) =>
      cr.focus(_.index).replace(idx)
    }
  implicit val te: TypedEncoder[TestData]    = shapeless.cachedImplicit
  implicit val hd: NJHeaderEncoder[TestData] = shapeless.cachedImplicit
  implicit val ri: RowDecoder[TestData]      = shapeless.cachedImplicit

  val codec: NJAvroCodec[TestData] = NJAvroCodec[TestData]

  val loader: LoadTable[IO, TestData] = sparkSession.loadTable[IO](AvroTypedEncoder[TestData](codec))

}

class ReadWriteTest extends AnyFunSuite {
  import ReadWriteTestData.*
  val hdp = sparkSession.hadoop[IO]
  test("circe write - read") {
    val path = NJPath("./data/test/spark/pipe/circe.json")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer = hdp.circe.sink(policy)(t => path / t.index)
    data.map(_.asJson).through(writer).compile.drain.unsafeRunSync()
    val count = loader.circe(path).count.unsafeRunSync()
    assert(count == number)
  }
  test("jackson write - read") {
    val path = NJPath("./data/test/spark/pipe/jackson.json")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer = hdp.jackson(codec.schema).sink(policy)(t => path / t.index)
    data.map(codec.toRecord).through(writer).compile.drain.unsafeRunSync()
    val count = loader.jackson(path).count.unsafeRunSync()
    assert(count == number)
  }
  test("kantan write - read") {
    val path = NJPath("./data/test/spark/pipe/kantan.csv")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer =
      hdp.kantan(CsvConfiguration.rfc).sink[TestData](policy)(t => path / t.index)
    data.through(writer).compile.drain.unsafeRunSync()
    val count = sparkSession
      .loadTable[IO](AvroTypedEncoder[TestData])
      .kantan(path, CsvConfiguration.rfc)
      .count
      .unsafeRunSync()
    assert(count == number)
  }
  test("avro write - read") {
    val path = NJPath("./data/test/spark/pipe/apache.avro")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer = hdp.avro(codec.schema).sink(policy)(t => path / t.index)
    data.map(codec.toRecord).through(writer).compile.drain.unsafeRunSync()
    val count = loader.avro(path).count.unsafeRunSync()
    assert(count == number)
  }
  test("bin-avro write - read") {
    val path = NJPath("./data/test/spark/pipe/bin.avro")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer = hdp.binAvro(codec.schema).sink(policy)(t => path / t.index)
    data.map(codec.toRecord).through(writer).compile.drain.unsafeRunSync()
    val count = loader.binAvro(path).count.unsafeRunSync()
    assert(count == number)
  }

  test("parquet write - read") {
    val path = NJPath("./data/test/spark/pipe/apache.parquet")
    hdp.delete(path).unsafeRunSync()
    val policy = RetryPolicies.constantDelay[IO](0.3.second)
    val writer = hdp.parquet(codec.schema).sink(policy)(t => path / t.index)
    data.map(codec.toRecord).through(writer).compile.drain.unsafeRunSync()
    val count = loader.parquet(path).count.unsafeRunSync()
    assert(count == number)
  }
}
