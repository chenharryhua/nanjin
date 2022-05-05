package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{sns, SimpleNotificationService}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*

import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object slack {
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]]): NJSlackPipe[F] =
    new NJSlackPipe[F](snsResource, None, None, Translator.slack[F])

  def apply[F[_]: Async](snsArn: SnsArn): NJSlackPipe[F] =
    new NJSlackPipe[F](sns[F](snsArn), None, None, Translator.slack[F])
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final class NJSlackPipe[F[_]](
  snsResource: Resource[F, SimpleNotificationService[F]],
  supporters: Option[String],
  interval: Option[FiniteDuration],
  translator: Translator[F, SlackApp])(implicit F: Async[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, SlackApp, NJSlackPipe[F]] {

  def withInterval(fd: FiniteDuration): NJSlackPipe[F] =
    new NJSlackPipe[F](snsResource, supporters, Some(fd), translator)

  /** supporters will be notified:
    *
    * ServicePanic
    *
    * ServiceStop
    *
    * ServiceTermination
    */
  def at(supporters: String): NJSlackPipe[F] = {
    val sp = Translator.servicePanic[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    val st = Translator.serviceStop[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    new NJSlackPipe[F](snsResource, Some(supporters), interval, sp.andThen(st)(translator))
  }

  override def updateTranslator(f: Translator[F, SlackApp] => Translator[F, SlackApp]): NJSlackPipe[F] =
    new NJSlackPipe[F](snsResource, supporters, interval, f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty))
      event <- es
        .evalTap(evt => updateRef(ref, evt))
        .evalTap(e =>
          translator.filter {
            case MetricReport(rt, ss, _, ts, _) =>
              isShowMetrics(ss.serviceParams.metric.reportSchedule, ts, interval, ss.launchTime) || rt.isShow
            case ActionStart(ai)            => ai.actionParams.isCritical
            case ActionSucc(ai, _, _, _)    => ai.actionParams.isCritical
            case ActionRetry(ai, _, _, _)   => ai.actionParams.isNotice
            case ActionFail(ai, _, _, _, _) => ai.actionParams.isNonTrivial
            case _                          => true
          }.translate(e).flatMap(_.traverse(sa => sns.publish(sa.asJson.noSpaces).attempt)).void)
        .onFinalize {
          serviceTerminateEvents(ref, translator)
            .flatMap(_.traverse(msg => sns.publish(msg.asJson.noSpaces).attempt))
            .void
        }
    } yield event
}
