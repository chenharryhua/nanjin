import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.sparkafka.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val topics = new Topics

  val payment = topics.payment.in(ctx)

  val sparkSettings: SparkSettings =
    SparkSettings.default.updateConf(_.setMaster("local[*]").setAppName("test"))

  val spark: SparkSession = sparkSettings.session
  //SparkSession.builder().master("local[*]").appName("test").getOrCreate()
}
