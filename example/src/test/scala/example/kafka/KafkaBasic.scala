package example.kafka

import cats.derived.auto.show._
import example._
import example.topics.fooTopic
import frameless.TypedEncoder
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

@DoNotDiscover
class KafkaBasic extends AnyFunSuite {
  implicit val foo: TypedEncoder[Foo] = shapeless.cachedImplicit

  test("send message to kafka topic") {
    fooTopic.send(1, Foo(1, "a")).unsafeRunSync()
  }

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
  test("transform and pipe to another topic") {
    val other = ctx.topic[Int, String]("example.pipe")
    fooTopic.fs2Channel.stream
      .map(x => fooTopic.decoder(x).decode)
      .evalMap(m => other.send(m.record.key, m.record.value.b))
      .interruptAfter(2.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  val path = "./data/example/foo.json"
  test("persist messages to local disk") {
    sparKafka.topic(fooTopic).fromKafka.save.circe(path).file.run(blocker).unsafeRunSync()
  }

  test("populate topic using persisted data") {
    sparKafka
      .topic(fooTopic)
      .load
      .circe(path)
      .prRdd
      .withBatchSize(2)
      .withInterval(1.second) // send 2 messages every 1 second
      .withTimeLimit(5.second) // upload last for 5 seconds
      .upload(akkaSystem)
      .compile
      .drain
      .unsafeRunSync()
  }
}
