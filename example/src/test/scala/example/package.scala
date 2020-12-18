import akka.actor.ActorSystem
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSettings}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object example {
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-example")

  implicit val sparkSession: SparkSession     = SparkSettings.default.session
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]               = IO.timer(global)

  val blocker: Blocker = Blocker.liftExecutionContext(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withApplicationId("nj-example-app")
      .withGroupId("nj-example-group")
      .ioContext

  val topic: KafkaTopic[IO, Int, String]             = ctx.topic[Int, String]("example.topic")
  val ate: AvroTypedEncoder[OptionalKV[Int, String]] = OptionalKV.ate(topic.topicDef)
}
