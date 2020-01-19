package mtest.spark

import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.common._
import com.github.chenharryhua.nanjin.database.Postgres
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaSession

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext

  val db: Postgres = Postgres(
    Username("postgres"),
    Password("postgres"),
    Host("localhost"),
    Port(5432),
    DatabaseName("postgres"))

  implicit val sparkSession =
    SparkSettings.default.withConf(_.setMaster("local[*]").setAppName("test-spark")).session

}
