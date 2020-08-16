package mtest.spark

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.database.{
  DatabaseName,
  Host,
  Password,
  Port,
  Postgres,
  Username
}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object database {
  val blocker: Blocker              = Blocker.liftExecutionContext(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  implicit val sparkSession: SparkSession =
    SparkSettings.default
      .withConfigUpdate(_.setMaster("local[*]").setAppName("test-spark"))
      .withLogLevel(NJLogLevel.ERROR)
      .session

  val postgres: Postgres = Postgres(
    Username("postgres"),
    Password("postgres"),
    Host("localhost"),
    Port(5432),
    DatabaseName("postgres"))
}
