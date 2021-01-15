package mtest

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings, KafkaTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {
  import akka.actor.ActorSystem
  implicit val cs: ContextShift[IO]    = IO.contextShift(global)
  implicit val timer: Timer[IO]        = IO.timer(global)
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-test")

  val blocker: Blocker = Blocker.liftExecutionContext(global)

  val ctx: KafkaContext[IO] =
    KafkaSettings.local
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .ioContext
      .withGroupId("nj-kafka-unit-test-group")
      .withApplicationId("nj-kafka-unit-test-app")

  val taxi: KafkaTopic[IO, Int, trip_record] =
    ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
}
