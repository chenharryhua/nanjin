package mtest.spark.pipe

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import com.sksamuel.avro4s.ToRecord
import frameless.TypedEncoder
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.typesafe.dsl.*
import kantan.csv.generic.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import monocle.syntax.all.*
import mtest.spark.sparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*

object ReadWriteTestData {
  final case class TestData(index: Long, name: String)
  val number       = 10000
  val cr: TestData = TestData(0, "abc")
  val data: Stream[IO, TestData] =
    Stream.emits(List.fill(number)(cr)).covary[IO].chunkLimit(2).unchunks.zipWithIndex.map { case (cr, idx) =>
      cr.focus(_.index).replace(idx)
    }
  implicit val te: TypedEncoder[TestData] = shapeless.cachedImplicit
  implicit val hd: RowEncoder[TestData]   = shapeless.cachedImplicit
  implicit val ri: RowDecoder[TestData]   = shapeless.cachedImplicit

  val codec: NJAvroCodec[TestData] = NJAvroCodec[TestData]
  val toRecord: ToRecord[TestData] = ToRecord(codec)

  val loader: LoadTable[TestData] = sparkSession.loadTable(AvroTypedEncoder[TestData](codec))

}

class ReadWriteTest extends AnyFunSuite {
  import ReadWriteTestData.*
  val hdp: NJHadoop[IO] = sparkSession.hadoop[IO]
  test("circe write - read") {
    val path = "./data/test/spark/pipe/circe.json"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer = hdp.circe.rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(_.asJson).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = loader.circe(path).count[IO].unsafeRunSync()
    assert(count == number)
  }
  test("jackson write - read") {
    val path = "./data/test/spark/pipe/jackson.json"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer = hdp.jackson(codec.schema).rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(toRecord.to).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = loader.jackson(path).count[IO].unsafeRunSync()
    assert(count == number)
  }
  test("kantan write - read") {
    val path = "./data/test/spark/pipe/kantan.csv"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer =
      hdp.kantan(CsvConfiguration.rfc).rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(hd.encode).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = sparkSession
      .loadTable(AvroTypedEncoder[TestData])
      .kantan(path, CsvConfiguration.rfc)
      .count[IO]
      .unsafeRunSync()
    assert(count == number)
  }
  test("avro write - read") {
    val path = "./data/test/spark/pipe/apache.avro"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer = hdp.avro(codec.schema).rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(toRecord.to).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = loader.avro(path).count[IO].unsafeRunSync()
    assert(count == number)
  }
  test("bin-avro write - read") {
    val path = "./data/test/spark/pipe/bin.avro"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer = hdp.binAvro(codec.schema).rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(toRecord.to).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = loader.binAvro(path).count[IO].unsafeRunSync()
    assert(count == number)
  }

  test("parquet write - read") {
    val path = "./data/test/spark/pipe/apache.parquet"
    hdp.delete(path).unsafeRunSync()
    val policy = Policy.fixedDelay(0.3.second)
    val writer = hdp.parquet(codec.schema).rotateSink(policy, ZoneId.systemDefault())(t => path / t.index)
    data.map(toRecord.to).chunks.through(writer).compile.drain.unsafeRunSync()
    val count = loader.parquet(path).count[IO].unsafeRunSync()
    assert(count == number)
  }
}
