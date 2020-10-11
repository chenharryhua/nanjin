package mtest.spark

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object persist {

  implicit val sparkSession: SparkSession =
    SparkSettings.default
      .withConfigUpdate(_.set("spark.sql.session.timeZone", "UTC"))
      .withLogLevel(NJLogLevel.ERROR)
      .withoutUI
      .session
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val blocker: Blocker = Blocker.liftExecutionContext(global)
}
