package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStart, ServiceStop, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.And
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.string.Trimmed

import java.time.{Duration, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object observers {
  type Title = Refined[String, NonEmpty And Trimmed]
  object Title extends RefinedTypeOps[Title, String] with CatsRefinedTypeOpsSyntax

  type Subject = Refined[String, NonEmpty And Trimmed]
  object Subject extends RefinedTypeOps[Subject, String] with CatsRefinedTypeOpsSyntax

  def isShowMetrics(
    reportSchedule: Option[Either[FiniteDuration, CronExpr]],
    now: ZonedDateTime,
    interval: Option[FiniteDuration],
    launchTime: ZonedDateTime): Boolean =
    interval match {
      case None => true
      case Some(iv) =>
        val border: ZonedDateTime =
          launchTime.plus(((Duration.between(launchTime, now).toScala / iv).toLong * iv).toJava)
        if (now === border) true
        else
          reportSchedule match {
            case None => true
            // true when now cross the border
            case Some(Left(fd))  => now.minus(fd.toJava).isBefore(border) && now.isAfter(border)
            case Some(Right(ce)) => ce.prev(now).forall(_.isBefore(border) && now.isAfter(border))
          }
    }

  final private[observers] val AbnormalTerminationMessage = "Service Termination Notice"

  private[observers] def serviceTerminateEvents[F[_], A](
    events: Ref[F, Map[UUID, ServiceStart]],
    translator: Translator[F, A])(implicit F: Temporal[F]): F[List[A]] =
    for {
      ts <- F.realTimeInstant
      msgs <- events.get.flatMap(
        _.values.toList.traverse(ss =>
          translator.translate(
            ServiceStop(
              ss.serviceStatus,
              ss.serviceParams.toZonedDateTime(ts),
              ServiceStopCause.Abnormally("external termination")))))
    } yield msgs.flatten

  private[observers] def updateRef[F[_]: Applicative](ref: Ref[F, Map[UUID, ServiceStart]], event: NJEvent): F[Unit] =
    event match {
      case ss: ServiceStart      => ref.update(_.updated(ss.serviceID, ss))
      case ServiceStop(ss, _, _) => ref.update(_.removed(ss.uuid))
      case _                     => Applicative[F].unit
    }
}
