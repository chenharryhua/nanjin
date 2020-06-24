package mtest

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object spark {
  implicit val sparkSession: SparkSession = SparkSettings.default.session
  implicit val cs: ContextShift[IO]       = IO.contextShift(global)
  implicit val timer: Timer[IO]           = IO.timer(global)

  val blocker: Blocker = Blocker.liftExecutionContext(global)

}
