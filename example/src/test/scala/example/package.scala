import akka.actor.ActorSystem
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object example {
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-example")

  implicit val sparkSession: SparkSession =
    SparkSettings.default.withLogLevel(NJLogLevel.ERROR).session

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]               = IO.timer(global)

  val blocker: Blocker = Blocker.liftExecutionContext(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withApplicationId("nj-example-app")
      .withGroupId("nj-example-group")
      .ioContext
}
