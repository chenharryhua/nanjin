package mtest

import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, KafkaTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.ExecutionContext.Implicits.global
import cats.derived.auto.show._
import cats.implicits._
import io.circe.generic.auto._
import com.github.chenharryhua.nanjin.kafka.TopicName


package object kafka {
  import akka.stream.ActorMaterializer
  import akka.actor.ActorSystem
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)
  implicit val akkaSystem           = ActorSystem("nj-test")
  // val materializer                  = ActorMaterializer.create(akkaSystem)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withGroupId("kafka.unit.test")
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .ioContext

  val taxi: KafkaTopic[IO, Int, trip_record] =
    ctx.topic[Int, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
}
