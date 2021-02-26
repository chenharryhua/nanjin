package example.kafka

import cats.derived.auto.show._
import example._
import example.topics.fooTopic
import frameless.TypedEncoder
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import cats.effect.unsafe.implicits.global

@DoNotDiscover
class KafkaBasic extends AnyFunSuite {
  implicit val foo: TypedEncoder[Foo] = shapeless.cachedImplicit

  test("consume message from kafka") {
    fooTopic.fs2Channel.stream
      .map(x => fooTopic.decoder(x).decode)
      .take(3)
      .showLinesStdOut
      .interruptAfter(2.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  val path = "./data/example/foo.json"
  test("persist messages to local disk") {
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.save.circe(path).file.run).unsafeRunSync()
  }

  test("populate topic using persisted data") {
    sparKafka
      .topic(fooTopic)
      .load
      .circe(path)
      .prRdd
      .withInterval(1.second) // interval of sending messages
      .withTimeLimit(5.second) // upload last for 5 seconds
      .uploadByBatch
      .withBatchSize(2)
      .run
      .compile
      .drain
      .unsafeRunSync()
  }
}
