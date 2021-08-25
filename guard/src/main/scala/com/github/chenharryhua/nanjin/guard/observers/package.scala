package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all.*
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{INothing, Pipe, Stream}
import io.circe.syntax.*
import org.log4s.Logger

package object observers {

  private[this] val logger: Logger = org.log4s.getLogger

  private def logging[F[_]](f: NJEvent => String)(implicit F: Sync[F]): Pipe[F, NJEvent, INothing] = {
    (events: Stream[F, NJEvent]) =>
      events.evalMap { event =>
        val out: String = f(event)
        event match {
          case ServicePanic(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case ActionRetrying(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.warn(out))(ex => logger.warn(ex)(out)))
          case ActionFailed(_, _, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case _ => F.blocking(logger.info(out))
        }
      }.drain
  }

  def showLog[F[_]: Sync]: Pipe[F, NJEvent, INothing] = logging[F](_.show)
  def jsonLog[F[_]: Sync]: Pipe[F, NJEvent, INothing] = logging[F](_.asJson.noSpaces)

  def showConsole[F[_]: Console]: Pipe[F, NJEvent, INothing] =
    _.evalMap(event => Console[F].println(event.show)).drain

  def jsonConsole[F[_]: Console]: Pipe[F, NJEvent, INothing] =
    _.evalMap(event => Console[F].println(event.asJson)).drain

  def cloudwatch[F[_]: Sync](client: Resource[F, CloudWatch[F]], namespace: String): CloudWatchMetrics[F] =
    new CloudWatchMetrics(client, namespace, 60, MetricFilter.ALL)

  def cloudwatch[F[_]: Sync](namespace: String): CloudWatchMetrics[F] =
    cloudwatch(CloudWatch[F], namespace)

}
