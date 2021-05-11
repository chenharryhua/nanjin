import akka.actor.ActorSystem
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import cats.effect.IO
import scala.concurrent.ExecutionContext.Implicits.global

package object example {
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-example")

  val sparkSession: SparkSession =
    SparkSettings.default.withLogLevel(NJLogLevel.ERROR).session

  val ctx: KafkaContext[IO] =
    KafkaSettings.local
      .withApplicationId("nj-example-app")
      .withGroupId("nj-example-group")
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withClientID("nj-example")
      .ioContext

  val sparKafka: SparKafkaContext[IO] = sparkSession.alongWith(ctx)
}
