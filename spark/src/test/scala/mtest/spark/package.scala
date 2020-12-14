package mtest

import akka.actor.ActorSystem
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object spark {
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-spark")

  implicit val sparkSession: SparkSession     = SparkSettings.default.session
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]               = IO.timer(global)

  val blocker: Blocker = Blocker.liftExecutionContext(global)

  val ctx: IoKafkaContext = KafkaSettings.local.withGroupId("spark.kafka.stream.test").ioContext

}
