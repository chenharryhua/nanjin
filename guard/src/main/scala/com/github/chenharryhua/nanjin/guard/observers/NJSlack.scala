package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{sns, SimpleNotificationService}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.*
import fs2.{Pipe, Stream}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object slack {
  private def defaultCfg[F[_]: Monad]: SlackConfig[F] = SlackConfig[F](
    reportInterval = None
  )
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]])(
    update: SlackConfig[F] => SlackConfig[F]): NJSlack[F] = {
    val cfg = update(defaultCfg)
    new NJSlack[F](snsResource, cfg, new SlackTranslator[F](cfg).translator)
  }

  def apply[F[_]: Async](snsArn: SnsArn)(update: SlackConfig[F] => SlackConfig[F]): NJSlack[F] = {
    val cfg = update(defaultCfg)
    new NJSlack[F](sns[F](snsArn), cfg, new SlackTranslator[F](cfg).translator)
  }
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final class NJSlack[F[_]] private[observers] (
  snsResource: Resource[F, SimpleNotificationService[F]],
  cfg: SlackConfig[F],
  translator: Translator[F, SlackApp])(implicit F: Async[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, SlackApp, NJSlack[F]] {

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def updateTranslator(f: Translator[F, SlackApp] => Translator[F, SlackApp]): NJSlack[F] =
    new NJSlack[F](snsResource, cfg, f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Set[ServiceParams]](Set.empty))
      event <- es.evalTap {
        case ServiceStart(_, _, params)   => ref.update(_.incl(params))
        case ServiceStop(_, _, params, _) => ref.update(_.excl(params))
        case _                            => F.unit
      }.evalTap(e => translator.translate(e).flatMap(_.traverse(sa => sns.publish(sa.asJson.noSpaces).attempt)).void)
        .onFinalize { // publish good bye message to slack
          for {
            services <- ref.get
            msg = SlackApp(
              username = "Service Termination Notice",
              attachments = List(
                Attachment(color = "", blocks = List(MarkdownSection(s":octagonal_sign: *Terminated Service(s)*")))) :::
                services.toList.map(ss => Attachment(color = "", blocks = List(hostServiceSection(ss))))
            ).asJson.spaces2
            _ <- sns.publish(msg).attempt.whenA(services.nonEmpty)
          } yield ()
        }
    } yield event
}
