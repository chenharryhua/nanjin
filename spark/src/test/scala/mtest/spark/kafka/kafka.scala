package mtest.spark

import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.database.{
  DatabaseName,
  Host,
  Password,
  Port,
  Postgres,
  Username
}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.withGroupId("spark.kafka.unit.test").ioContext

  val db: Postgres = Postgres(
    Username("postgres"),
    Password("postgres"),
    Host("localhost"),
    Port(5432),
    DatabaseName("postgres"))

  implicit val sparkSession: SparkSession =
    SparkSettings.default
      .withConfigUpdate(_.setMaster("local[*]").setAppName("test-spark"))
      .withLogLevel(NJLogLevel.ERROR)
      .session

}
