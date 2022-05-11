package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*

import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object SlackObserver {
  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): SlackObserver[F] =
    new SlackObserver[F](client, None, Translator.slack[F])
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final class SlackObserver[F[_]](
  client: Resource[F, SimpleNotificationService[F]],
  metricsInterval: Option[FiniteDuration],
  translator: Translator[F, SlackApp])(implicit F: Async[F])
    extends UpdateTranslator[F, SlackApp, SlackObserver[F]] {

  private def copy(
    metricsInterval: Option[FiniteDuration] = metricsInterval,
    translator: Translator[F, SlackApp] = translator): SlackObserver[F] =
    new SlackObserver[F](client, metricsInterval, translator)

  def withInterval(fd: FiniteDuration): SlackObserver[F] = copy(metricsInterval = Some(fd))

  /** supporters will be notified:
    *
    * ServicePanic
    *
    * ServiceStop
    *
    * ServiceTermination
    */
  def at(supporters: String): SlackObserver[F] = {
    val sp = Translator.servicePanic[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    val st = Translator.serviceStop[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    copy(translator = sp.andThen(st)(translator))
  }

  override def updateTranslator(f: Translator[F, SlackApp] => Translator[F, SlackApp]): SlackObserver[F] =
    copy(translator = f(translator))

  def observe(snsArn: SnsArn): Pipe[F, NJEvent, NJEvent] = (es: Stream[F, NJEvent]) =>
    for {
      sns <- Stream.resource(client)
      ref <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(r => new ObserverFinalizeMonitor(translator, r)))
      event <- es
        .evalTap(ref.monitoring)
        .evalTap(e =>
          translator.filter {
            case MetricReport(mrt, sp, _, ts, _, _) =>
              isShowMetrics(sp.metric.reportSchedule, ts, metricsInterval, sp.launchTime) || mrt.isShow
            case ActionStart(ai)            => ai.actionParams.isCritical
            case ActionSucc(ai, _, _, _)    => ai.actionParams.isCritical
            case ActionRetry(ai, _, _, _)   => ai.actionParams.isNotice
            case ActionFail(ai, _, _, _, _) => ai.actionParams.isNonTrivial
            case _                          => true
          }.translate(e).flatMap(_.traverse(sa => sns.publish(snsArn, sa.asJson.noSpaces).attempt)).void)
        .onFinalizeCase(
          ref.terminated(_).flatMap(_.traverse(msg => sns.publish(snsArn, msg.asJson.noSpaces).attempt)).void)
    } yield event
}
