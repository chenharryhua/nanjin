import cats.effect.{ContextShift, IO, Resource, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, KafkaTopic}
import com.github.chenharryhua.nanjin.sparkafka.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val topics = new Topics

  val payment: KafkaTopic[IO, String, Payment]  = topics.payment.in(ctx)
  val pencil_topic: KafkaTopic[IO, Int, Pencil] = ctx.topic[Int, Pencil]("pencile")
  val trip                                      = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")

  val spark: Resource[IO, SparkSession] =
    SparkSettings.default.updateConf(_.setMaster("local[*]").setAppName("test")).sessionResource[IO]

  val first_topic  = ctx.topic[Int, FirstStream]("first")
  val second_topic = ctx.topic[Int, SecondStream]("second")

}
