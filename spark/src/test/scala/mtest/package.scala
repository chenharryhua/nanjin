import cats.effect.{ContextShift, IO, Resource, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings, TopicDef}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import com.github.chenharryhua.nanjin.spark.kafka.{SparKafkaSession, SparKafkaSettings}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.kafka._
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {

  import com.github.chenharryhua.nanjin.spark.database.Postgres

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val db: Postgres = Postgres(
    Username("postgres"),
    Password("postgres"),
    Host("localhost"),
    Port(5432),
    DatabaseName("postgres"))

  val sparkSession =
    SparkSettings.default
      .update(_.setMaster("local[*]").setAppName("test-spark"))
      .sessionResource[IO]

  val sparKafkaSession: Resource[IO, SparKafkaSession] =
    SparKafkaSettings.default
      .updateSpark(_.setMaster("local[*]").setAppName("test-spark-kafka"))
      .sessionResource

}
