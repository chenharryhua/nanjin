package mtest.spark

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark._
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object sstream {
  implicit val sparkSession: SparkSession = SparkSettings.default.session
  implicit val cs: ContextShift[IO]       = IO.contextShift(global)
  implicit val timer: Timer[IO]           = IO.timer(global)

  val blocker: Blocker    = Blocker.liftExecutionContext(global)
  val ctx: IoKafkaContext = KafkaSettings.local.withGroupId("spark.kafka.stream.test").ioContext

  val sparKafka: SparkWithKafkaContext[IO] = sparkSession.alongWith(ctx)

}
