package example.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.*
import example.topics.fooTopic
import io.circe.generic.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

@DoNotDiscover
class ExampleKafkaBasic extends AnyFunSuite {
  test("populate topic") {
    val producerRecords: List[NJProducerRecord[Int, Foo]] =
      List(
        NJProducerRecord(fooTopic.topicName, 1, Foo(10, "a")),
        NJProducerRecord(fooTopic.topicName, 2, Foo(20, "b")),
        NJProducerRecord(fooTopic.topicName, 3, Foo(30, "c")),
        NJProducerRecord(fooTopic.topicName, 4, Foo(40, "d"))
      )
    sparKafka
      .topic(fooTopic)
      .prRdd(producerRecords)
      .producerRecords[IO](100)
      .through(fooTopic.produce.pipe)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("consume messages from kafka using https://fd4s.github.io/fs2-kafka/") {
    ctx
      .consume(fooTopic.topicName)
      .stream
      .map(fooTopic.serde.deserializeValue(_))
      .debug()
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("persist messages to local disk and then load data back into kafka") {
    val path = NJPath("./data/example/foo.json")
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.output.circe(path).run[IO]).unsafeRunSync()
    sparKafka
      .topic(fooTopic)
      .load
      .circe(path)
      .prRdd
      .producerRecords[IO](2)
      .through(fooTopic.produce.pipe)
      .compile
      .drain
      .unsafeRunSync()
  }
}
