package example.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic, ProtoTopic}
import eu.timepit.refined.auto.*
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import io.circe.generic.JsonCodec
object ProtobufData {
  val lions: List[Lion] =
    (1 to 10).map(_ => Lion("Melbourne Zoo", Random.nextInt())).toList

  val herd: List[Lion] = (1 to 10000).map(Lion("ChengDu Zoo", _)).toList

}
@JsonCodec
final case class Cub(name: String, age: Int)

class KafkaTopicTest extends AnyFunSuite {
  private val protoTopic = ProtoTopic[Int, Lion](TopicName("protobuf-example"))

  test("protobuf") {
    val lion = Lion("a", Random.nextInt())
    example.ctx.produce(protoTopic).produceOne(1, lion).unsafeRunSync()

    val res = example.ctx
      .consume(protoTopic)
      .circumscribedStream(DateTimeRange(sydneyTime))
      .flatMap(_.stream)
      .map(_.record.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res == lion)
  }

  test("json") {
    val topic = JsonTopic[Int, Cub](TopicName("json-example"))
    val lion = Cub("b", Random.nextInt())
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
    val topic = AvroTopic[Int, Cub](TopicName("avro-example-2"))
    val lion = Cub("c", Random.nextInt())
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
