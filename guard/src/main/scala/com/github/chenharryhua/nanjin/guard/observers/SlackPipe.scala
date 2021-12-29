package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{sns, SimpleNotificationService}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object slack {
  private def defaultCfg[F[_]: Monad]: SlackConfig[F] = SlackConfig[F](
    goodColor = "#36a64f",
    warnColor = "#ffd79a",
    infoColor = "#b3d1ff",
    errorColor = "#935252",
    metricsReportEmoji = ":eyes:",
    startActionEmoji = "",
    succActionEmoji = "",
    failActionEmoji = "",
    retryActionEmoji = "",
    durationFormatter = DurationFormatter.defaultFormatter,
    reportInterval = None,
    isShowRetry = false,
    extraSlackSections = Monad[F].pure(Nil),
    isLoggging = false,
    supporters = Nil,
    isShowMetrics = false
  )
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]])(
    update: SlackConfig[F] => SlackConfig[F]): SlackPipe[F] = {
    val cfg = update(defaultCfg)
    new SlackPipe[F](snsResource, cfg, new DefaultSlackTranslator[F](cfg).translator)
  }

  def apply[F[_]: Async](snsArn: SnsArn)(update: SlackConfig[F] => SlackConfig[F]): SlackPipe[F] = {
    val cfg = update(defaultCfg)
    new SlackPipe[F](sns[F](snsArn), cfg, new DefaultSlackTranslator[F](cfg).translator)
  }
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final class SlackPipe[F[_]] private[observers] (
  snsResource: Resource[F, SimpleNotificationService[F]],
  cfg: SlackConfig[F],
  translator: Translator[F, SlackApp])(implicit F: Async[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, SlackApp, SlackPipe[F]] {

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def updateTranslator(f: Translator[F, SlackApp] => Translator[F, SlackApp]): SlackPipe[F] =
    new SlackPipe[F](snsResource, cfg, f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Set[ServiceInfo]](Set.empty))
      event <- es.evalTap {
        case ServiceStarted(info, _)    => ref.update(_.incl(info))
        case ServiceStopped(info, _, _) => ref.update(_.excl(info))
        case _                          => F.unit
      }.evalTap(e =>
        translator
          .translate(e)
          .flatMap(_.traverse { sa =>
            logger.info(sa.asJson.spaces2).whenA(cfg.isLoggging) <*
              sns.publish(sa.asJson.noSpaces).attempt
          })
          .void)
        .onFinalize { // publish good bye message to slack
          for {
            ts <- F.realTimeInstant
            services <- ref.get
            msg = SlackApp(
              username = "Service Termination Notice",
              attachments = List(
                Attachment(
                  color = cfg.warnColor,
                  blocks = List(MarkdownSection(s":octagonal_sign: *Terminated Service(s)* ${cfg.atSupporters}")))) :::
                services.toList.map(ss =>
                  Attachment(
                    color = cfg.warnColor,
                    blocks = List(
                      hostServiceSection(ss.serviceParams),
                      JuxtaposeSection(
                        TextField("Up Time", cfg.durationFormatter.format(ss.launchTime.toInstant, ts)),
                        TextField("App", ss.serviceParams.taskParams.appName))
                    )
                  ))
            ).asJson.spaces2
            _ <- sns.publish(msg).attempt.whenA(services.nonEmpty)
            _ <- logger.info(msg).whenA(cfg.isLoggging)
          } yield ()
        }
    } yield event
}
