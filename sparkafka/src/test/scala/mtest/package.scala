import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext
  val topic               = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")

  val spark: SparkSession =
    SparkSession.builder().master("local[*]").appName("test").getOrCreate()
}
