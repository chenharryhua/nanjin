package example.spark

import cats.effect.unsafe.implicits.global
import example._
import example.topics.fooTopic
import frameless.TypedEncoder
import io.circe.generic.auto._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

@DoNotDiscover
class ExampleKafkaStructuredStream extends AnyFunSuite {
  test("persist messages using structured streaming") {
    val path: String = "./data/example/foo/sstream"
    sparKafka
      .topic(fooTopic)
      .sstream
      .datePartitionSink(path)
      .parquet
      .queryStream
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
