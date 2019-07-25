package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.FunSuite
import cats.implicits._

import scala.concurrent.ExecutionContext.Implicits.global

class ConsumerMessageTest extends FunSuite {
  lazy val system: ActorSystem                      = ActorSystem("akka-kafka")
  implicit lazy val materializer: ActorMaterializer = ActorMaterializer.create(system)

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val conf  = KafkaSettings.empty.brokers("localhost:9092").groupId("test")
  val ctx   = new KafkaContext(conf)
  val topic = ctx.topic[String, String](KafkaTopicName("backblaze_smart"))

  test("consume string") {
    topic
      .fs2Stream[IO]
      .consumeValidMessages
      .map(_.toString)
      .showLinesStdOut
      .compile
      .drain
      .unsafeRunSync()
  }
  test("akka consumer") {
    val ggg = topic.akkaStream.consumeValidMessages.map(_.toString)

  }
}
