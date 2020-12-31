package mtest.spark.sstream

import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedDataset
import mtest.spark.{contextShift, sparKafka, sparkSession, timer}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import org.apache.spark.sql.SparkSession

object StreamJoinTestData {
  implicit val ss: SparkSession = sparkSession
  final case class Foo(index: Int, name: String)

  final case class Bar(index: Int, age: Int)

  final case class FooBar(index: Int, name: String, age: Int)

  val barDS: Dataset[Bar] = TypedDataset.create(List(Bar(1, 1), Bar(2, 2), Bar(3, 3))).dataset

  val fooTopic = sparKafka.topic[Int, Foo]("spark.stream.table.join.test")

  val fooData: List[NJProducerRecord[Int, Foo]] = List
    .fill(50)(Foo(0, "a"))
    .zipWithIndex
    .map { case (foo, idx) => foo.copy(index = idx) }
    .map(x => NJProducerRecord[Int, Foo](x))
}

class StreamJoinTest extends AnyFunSuite {

  import StreamJoinTestData._
  import sparkSession.implicits._
  test("stream-table join") {
    val path   = "./data/test/spark/sstream/stream-table-join"
    val sender = fooTopic.prRdd(fooData).triggerEvery(1.seconds).upload

    val ss =
      fooTopic.sstream.transform { fooDS =>
        fooDS.joinWith(barDS, fooDS("value.index") === barDS("index"), "inner").flatMap { case (foo, bar) =>
          foo.value.map(x => FooBar(bar.index, x.name, bar.age))
        }
      }.fileSink(path).triggerEvery(1.seconds).queryStream

    ss.concurrently(sender).interruptAfter(8.seconds).compile.drain.unsafeRunSync()

    val ate = AvroTypedEncoder[FooBar]
    val res = loaders.json(path, ate, sparkSession).dataset.map(_.age).distinct().collect().toSet
    assert(res == Set(1, 2, 3))
  }
}
