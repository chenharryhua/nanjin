package com.github.chenharryhua.nanjin.guard

import cats.Endo
import cats.effect.kernel.Async
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{Service, ServiceConfig, Task}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import fs2.io.net.Network

/** Top-level entry point for nanjin's service supervision framework.
  *
  * A `TaskGuard` represents a named task — a logical grouping of one or more services. It holds shared
  * configuration (time zone, log format, metrics reporting policy, restart policy) that is inherited by all
  * services created from it.
  *
  * ===Typical usage===
  *
  * {{{
  * import cats.effect.{IO, IOApp}
  * import com.github.chenharryhua.nanjin.guard.TaskGuard
  * import scala.concurrent.duration.*
  *
  * object Main extends IOApp.Simple {
  *   val task = TaskGuard[IO]("my-app")
  *     .updateConfig(
  *       _.withZoneId(java.time.ZoneId.of("Australia/Sydney"))
  *         .withRestartPolicy(1.minute, _.fixedDelay(5.seconds))
  *         .withMetricsReport(_.crontab(_.hourly))
  *     )
  *
  *   val run: IO[Unit] =
  *     task.service("worker").eventStream { agent =>
  *       // application logic
  *       IO.unit
  *     }.compile.drain
  * }
  * }}}
  *
  * ===Design===
  *
  * `TaskGuard` itself is lightweight and stateless — it only carries configuration. The actual service
  * lifecycle (restart supervision, metrics collection, event publishing) begins when the event stream
  * returned by [[ServiceGuard.eventStream]] is compiled.
  *
  * Multiple services can be created from the same `TaskGuard`, sharing the same task name and base
  * configuration but running independently with their own service IDs and metrics registries.
  */
final class TaskGuard[F[_]: {Async, Network, Console}] private (serviceConfig: ServiceConfig[F])
    extends UpdateConfig[ServiceConfig[F], TaskGuard[F]] {

  override def updateConfig(f: Endo[ServiceConfig[F]]): TaskGuard[F] =
    new TaskGuard[F](f(serviceConfig))

  def service(serviceName: String): ServiceGuard[F] =
    ServiceGuard[F](
      serviceName = Service(serviceName),
      config = serviceConfig
    )
}

object TaskGuard {

  def apply[F[_]: {Async, Network, Console}](taskName: String): TaskGuard[F] =
    new TaskGuard[F](ServiceConfig(Task(taskName)))

}
