package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{sns, SimpleNotificationService}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*

import scala.concurrent.duration.FiniteDuration

object slack {
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]]): NJSlack[F] =
    new NJSlack[F](snsResource, None, Translator.slack[F])

  def apply[F[_]: Async](snsArn: SnsArn): NJSlack[F] =
    new NJSlack[F](sns[F](snsArn), None, Translator.slack[F])
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final class NJSlack[F[_]] private[observers] (
  snsResource: Resource[F, SimpleNotificationService[F]],
  interval: Option[FiniteDuration],
  translator: Translator[F, SlackApp])(implicit F: Async[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, SlackApp, NJSlack[F]] {

  def withInterval(fd: FiniteDuration): NJSlack[F] = new NJSlack[F](snsResource, Some(fd), translator)

  def at(supporters: String): NJSlack[F] = {
    val sp = Translator.servicePanic[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    updateTranslator(sp)
  }

  override def updateTranslator(f: Translator[F, SlackApp] => Translator[F, SlackApp]): NJSlack[F] =
    new NJSlack[F](snsResource, interval, f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Set[ServiceParams]](Set.empty))
      event <- es.evalTap {
        case ServiceStart(_, _, params)   => ref.update(_.incl(params))
        case ServiceStop(_, _, _, params) => ref.update(_.excl(params))
        case _                            => F.unit
      }.evalTap(e =>
        translator.filter {
          case MetricReport(rt, ss, _, ts, sp, _) =>
            isShowMetrics(
              sp.metric.reportSchedule,
              sp.toZonedDateTime(ts),
              interval,
              sp.toZonedDateTime(ss.launchTime)) || rt.isShow
          case ActionStart(ai)            => ai.isCritical
          case ActionSucc(ai, _, _, _)    => ai.isCritical
          case ActionRetry(ai, _, _, _)   => ai.isNotice
          case ActionFail(ai, _, _, _, _) => ai.nonTrivial
          case _                          => true
        }.translate(e).flatMap(_.traverse(sa => sns.publish(sa.asJson.noSpaces).attempt)).void)
        .onFinalize { // publish good bye message to slack
          for {
            services <- ref.get
            msg = SlackApp(
              username = "Service Termination Notice",
              attachments = List(
                Attachment(
                  color = "#ffd79a",
                  blocks = List(MarkdownSection(s":octagonal_sign: *Terminated Service(s)*")))) :::
                services.toList.map(ss => Attachment(color = "#ffd79a", blocks = List(hostServiceSection(ss))))
            )
            _ <- sns.publish(msg.asJson.noSpaces).attempt.whenA(services.nonEmpty)
          } yield ()
        }
    } yield event
}
