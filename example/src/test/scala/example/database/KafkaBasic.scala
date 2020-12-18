package example.database

import cats.derived.auto.show._
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopicSyntax
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

  test("consume message") {
    fooTopic.fs2Channel.stream
      .map(x => fooTopic.decoder(x).decode)
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
  test("dump messages") {
    fooTopic.sparKafka.fromKafka
      .flatMap(_.save.circe("./data/example/dump.json").gzip.run(blocker))
      .unsafeRunSync()
  }
  test("populate topic using saved data") {
    fooTopic.sparKafka.load
      .circe("./data/example/dump.json")
      .prRdd
      .batch(1) // send 1 message
      .interval(1) // every 1 second
      .upload
      .compile
      .drain
      .unsafeRunSync()
  }
}
