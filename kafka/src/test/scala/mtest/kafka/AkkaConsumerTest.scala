package mtest.kafka

import cats.effect.IO
import org.scalatest.funsuite.AnyFunSuite
import fs2.Stream
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import fs2.kafka.{ProducerRecord, ProducerResult}

import scala.concurrent.duration.DurationInt
import io.scalaland.chimney.dsl._

class AkkaConsumerTest extends AnyFunSuite {
  val topic = ctx.topic[Int, String]("akka.consumer.test")

  val data: List[ProducerRecord[Int, String]] = List(
    topic.fs2PR(1, "a"),
    topic.fs2PR(2, "b"),
    topic.fs2PR(3, "c"),
    topic.fs2PR(4, "d"),
    topic.fs2PR(5, "e"))

  val sender: Stream[IO, List[ProducerResult[Int, String, Unit]]] =
    Stream.awakeEvery[IO](1.second).zipRight(Stream.eval(data.traverse(x => topic.send(x))))

  test("time-ranged") {
    val akkaChannel = topic.akkaChannel
    val range       = NJDateTimeRange(sydneyTime)
    (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> IO.sleep(1.seconds))
      .unsafeRunSync()
    val res = akkaChannel
      .timeRanged(range)
      .delayBy(2.seconds)
      .map(x => topic.decoder(x).decode)
      .concurrently(sender)
      .interruptAfter(18.seconds)
      .compile
      .toList
      .unsafeRunSync()
      .map(x => ProducerRecord(x.topic, x.key(), x.value()))
    assert(res == data)
  }
}
