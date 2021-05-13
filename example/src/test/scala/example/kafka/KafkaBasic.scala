package example.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
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

  test("consume messages from kafka using https://fd4s.github.io/fs2-kafka/") {
    fooTopic.fs2Channel.stream
      .map(x => fooTopic.decoder(x).decode)
      .take(3)
      .debug()
      .interruptAfter(2.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("persist messages to local disk and load the data into kafka") {
    val path = "./data/example/foo.json"
    val save = sparKafka.topic(fooTopic).fromKafka.flatMap(_.save.circe(path).file.run)
    val load = sparKafka
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

    (save >> load).unsafeRunSync()
  }

  test("persist messages using dstream") {
    val path   = "./data/example/foo/dstream"
    val runner = DStreamRunner[IO](sparKafka.sparkSession.sparkContext, "./data/example/foo/checkpoint", 2.seconds)
    runner
      .signup(sparKafka.topic(fooTopic).dstream)(_.coalesce.circe(path))
      .run
      .interruptAfter(10.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

}
