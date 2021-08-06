package mtest

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.alert.{
  AlertService,
  ConsoleService,
  LogService,
  MetricsService,
  SlackService
}

import scala.concurrent.duration.*

package object guard {
  val metrics: Resource[IO, AlertService[IO]] = MetricsService.consoleReporter[IO](1.second)
  val log: AlertService[IO]                   = LogService[IO]
  val console: AlertService[IO]               = ConsoleService[IO]
  val slack: Resource[IO, AlertService[IO]]   = SlackService[IO](SimpleNotificationService.fake[IO])
}
