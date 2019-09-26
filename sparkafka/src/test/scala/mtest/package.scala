import cats.effect.{ContextShift, IO, Resource, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.sparkdb.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val spark: Resource[IO, SparkSession] =
    SparkSettings.default.updateConf(_.setMaster("local[*]").setAppName("test")).sessionResource[IO]

}
