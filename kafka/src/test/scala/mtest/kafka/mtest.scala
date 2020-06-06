package mtest

import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, KafkaTopic, TopicName}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {
  import akka.actor.ActorSystem
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)
  implicit val akkaSystem           = ActorSystem("nj-test")
  // val materializer                  = ActorMaterializer.create(akkaSystem)

  val blocker = Blocker.liftExecutionContext(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withGroupId("kafka.unit.test")
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .ioContext

  val taxi: KafkaTopic[IO, Int, trip_record] =
    ctx.topic[Int, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
}
