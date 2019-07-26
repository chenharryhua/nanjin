package com.github.chenharryhua.nanjin

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {
  lazy val system: ActorSystem = ActorSystem("akka-kafka")
  implicit lazy val materializer: ActorMaterializer =
    ActorMaterializer.create(system)

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: KafkaContext =
    KafkaSettings.empty
      .brokers("localhost:9092")
      .groupId("test")
      .applicationId("test-stream")
      .schemaRegistryUrl("http://localhost:8081")
      .context
}
