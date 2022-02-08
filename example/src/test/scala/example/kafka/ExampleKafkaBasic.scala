package example.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.kafka.NJProducerRecord
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
        NJProducerRecord(1, Foo(10, "a")),
        NJProducerRecord(2, Foo(20, "b")),
        NJProducerRecord(3, Foo(30, "c")),
        NJProducerRecord(4, Foo(40, "d")))
    sparKafka
      .topic(fooTopic)
      .prRdd(producerRecords)
      .producerRecords(fooTopic.topicName, 100)
      .through(fooTopic.fs2Channel.producerPipe)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("consume messages from kafka using https://fd4s.github.io/fs2-kafka/") {
    fooTopic.fs2Channel.stream
      .map(x => fooTopic.decoder(x).decode)
      .debug()
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("persist messages to local disk and then load data back into kafka") {
    val path = NJPath("./data/example/foo.json")
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.save(path).circe.run).unsafeRunSync()
    sparKafka
      .topic(fooTopic)
      .load
      .circe(path)
      .flatMap(_.prRdd.producerRecords(fooTopic.topicName, 2).through(fooTopic.fs2Channel.producerPipe).compile.drain)
      .unsafeRunSync()
  }
}
