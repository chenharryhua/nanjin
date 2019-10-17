import cats.effect.{ContextShift, IO, Resource, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, TopicDef}
import com.github.chenharryhua.nanjin.sparkafka._
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val spark: Resource[IO, SparKafkaSession] =
  SparKafkaSettings.default.updateSparkConf(_.setMaster("local[*]").setAppName("test")).sessionResource

}
