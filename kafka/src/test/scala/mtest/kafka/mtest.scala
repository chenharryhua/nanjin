package mtest

import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, KafkaTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.ExecutionContext.Implicits.global
import cats.derived.auto.show._
import cats.implicits._
import io.circe.generic.auto._ 

package object kafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .ioContext

  val taxi: KafkaTopic[IO, Int, trip_record] =
    ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
}
