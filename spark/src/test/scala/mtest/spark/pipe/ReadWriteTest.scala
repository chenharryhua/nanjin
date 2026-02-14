package mtest.spark.pipe

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{LoadDataset, SparkSessionExt}
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.ToRecord
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
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
  val number = 10000
  val cr: TestData = TestData(0, "abc")
  val data: Stream[IO, TestData] =
    Stream.emits(List.fill(number)(cr)).covary[IO].chunkLimit(2).unchunks.zipWithIndex.map { case (cr, idx) =>
      cr.focus(_.index).replace(idx)
    }
  implicit val hd: RowEncoder[TestData] = shapeless.cachedImplicit
  implicit val ri: RowDecoder[TestData] = shapeless.cachedImplicit

  val codec: AvroCodec[TestData] = AvroCodec[TestData]
  val toRecord: ToRecord[TestData] = ToRecord(codec)
  import sparkSession.implicits.*

  def loader(url: Url): LoadDataset[TestData] = sparkSession.loadDataset[TestData](url)

}

class ReadWriteTest extends AnyFunSuite {
  import ReadWriteTestData.*
  val hdp: Hadoop[IO] = sparkSession.hadoop[IO]
  test("circe write - read") {
    val path = "./data/test/spark/pipe/circe.json"
    hdp.delete(path).unsafeRunSync()
    val writer = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index).circe
    data.map(_.asJson).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).circe.count()
    assert(count == number)
  }
  test("jackson write - read") {
    val path = "./data/test/spark/pipe/jackson.json"
    hdp.delete(path).unsafeRunSync()
    val writer = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index).jackson
    data.map(toRecord.to).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).jackson.count()
    assert(count == number)
  }
  test("kantan write - read") {
    val path = "./data/test/spark/pipe/kantan.csv"
    hdp.delete(path).unsafeRunSync()
    val writer =
      hdp
        .rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index)
        .kantan(CsvConfiguration.rfc)
    data.map(hd.encode).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).kantan(CsvConfiguration.rfc).count()
    assert(count == number)
  }
  test("avro write - read") {
    val path = "./data/test/spark/pipe/apache.avro"
    hdp.delete(path).unsafeRunSync()
    val writer = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index).avro
    data.map(toRecord.to).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).avro.count()
    assert(count == number)
  }
  test("bin-avro write - read") {
    val path = "./data/test/spark/pipe/bin.avro"
    hdp.delete(path).unsafeRunSync()
    val writer = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index).binAvro
    data.map(toRecord.to).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).binAvro.count()
    assert(count == number)
  }

  test("parquet write - read") {
    val path = "./data/test/spark/pipe/apache.parquet"
    hdp.delete(path).unsafeRunSync()
    val writer = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.3.second))(t => path / t.index).parquet
    data.map(toRecord.to).through(writer).compile.drain.unsafeRunSync()
    val count = loader(path).parquet.count()
    assert(count == number)
  }
}
