package mtest

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.sinks.{jsonConsole, showLog, AlertService, SlackService}

import scala.concurrent.duration.*

package object guard {
  val slack: Resource[IO, AlertService[IO]] = SlackService[IO](SimpleNotificationService.fake[IO])
}
