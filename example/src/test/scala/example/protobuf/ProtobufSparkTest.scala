package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic, ProtobufTopic}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import eu.timepit.refined.auto.*
import example.sparkSession
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
object ProtobufData {
  val lions: List[Lion] =
    (1 to 10).map(_ => Lion("Melbourne Zoo", Random.nextInt())).toList

  val herd: List[Lion] = (1 to 10000).map(Lion("ChengDu Zoo", _)).toList

//  import sparkSession.implicits.*
//  val rdd = sparkSession.createDataset(lions).rdd
}
final case class JsonLion(name: String, age: Int)
final case class AvroLion(name: String, age: Int)
class ProtobufSparkTest extends AnyFunSuite {
//  import ProtobufData.*
  val hdp = sparkSession.hadoop[IO]

  // val topic = sparKafka.topic(TopicDef[Int, Lion](TopicName("test")))

  test("protobuf") {
    val topic = ProtobufTopic[Int, Lion](TopicName("protobuf-example"))
    val lion = Lion("Michael", 12)
    example.ctx.produce(topic).produceOne(1, lion).unsafeRunSync()

    val res = example.ctx
      .consume(topic)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res == lion)
  }

  test("json") {
    val topic = JsonTopic[Int, JsonLion](TopicName("json-example"))
    val lion = JsonLion("Michael", 12)
    example.ctx.produce(topic).produceOne(1, lion).unsafeRunSync()

    val res = example.ctx
      .consume(topic)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res == lion)
  }

  test("avro") {
    val topic = AvroTopic[Int, AvroLion](TopicName("avro-example"))
    val lion = AvroLion("Michael", 12)
    example.ctx.produce(topic).produceOne(1, lion).unsafeRunSync()

    val res = example.ctx
      .consume(topic)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res == lion)
  }
}
