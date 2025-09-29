package example.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import eu.timepit.refined.auto.*
import example.*
import example.topics.fooTopic
import fs2.Stream
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
@DoNotDiscover
class ExampleKafkaBasic extends AnyFunSuite {
  val topic = AvroTopic[Int, Foo](TopicName("foo"))

  test("populate topic") {
    val producerRecords: List[NJProducerRecord[Int, Foo]] =
      List(
        NJProducerRecord(fooTopic.topicName, 1, Foo(10, "a")),
        NJProducerRecord(fooTopic.topicName, 2, Foo(20, "b")),
        NJProducerRecord(fooTopic.topicName, 3, Foo(30, "c")),
        NJProducerRecord(fooTopic.topicName, 4, Foo(40, "d"))
      )
    val run =
      ctx
        .admin(fooTopic.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence)
        .attempt >>
        Stream
          .emits(producerRecords)
          .map(_.toProducerRecord)
          .chunks
          .through(ctx.sharedProduce[Int, Foo](topic.pair).sink)
          .compile
          .drain

    run.unsafeRunSync()
  }

  test("consume messages from kafka using https://fd4s.github.io/fs2-kafka/") {
    ctx
      .consumeGenericRecord(fooTopic)
      .subscribe
      .debug()
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
