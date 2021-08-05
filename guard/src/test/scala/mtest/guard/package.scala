package mtest

import cats.effect.IO
import cats.effect.kernel.Resource
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.alert.{
  AlertService,
  ConsoleService,
  LogService,
  MetricsService,
  SlackService
}

package object guard {
  val metricRegistry: MetricRegistry        = new MetricRegistry
  val metrics: AlertService[IO]             = MetricsService[IO](metricRegistry)
  val log: AlertService[IO]                 = LogService[IO]
  val console: AlertService[IO]             = ConsoleService[IO]
  val slack: Resource[IO, AlertService[IO]] = SlackService[IO](SimpleNotificationService.fake[IO])
}
