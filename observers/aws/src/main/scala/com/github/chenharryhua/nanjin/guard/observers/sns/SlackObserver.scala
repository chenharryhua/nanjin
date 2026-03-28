package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource, Temporal}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.aws.{SimpleNotificationService, SnsArn}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceId}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe}
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*
import software.amazon.awssdk.services.sns.model.PublishRequest

object SlackObserver {
  def apply[F[_]: Temporal](client: Resource[F, SimpleNotificationService[F]]): SlackObserver[F] =
    new SlackObserver[F](client, SlackTranslator[F], AlarmLevel.Warn)
}

/** Notes: Slack messages `https://api.slack.com/docs/messages/builder`
  */

final class SlackObserver[F[_]: Clock](
  client: Resource[F, SimpleNotificationService[F]],
  translator: Translator[F, SlackApp],
  threshold: AlarmLevel)(using F: Concurrent[F])
    extends UpdateTranslator[F, SlackApp, SlackObserver[F]] {

  def withAlarmLevel(f: AlarmLevel.type => AlarmLevel): SlackObserver[F] =
    new SlackObserver[F](client, translator, f(AlarmLevel))

  override def updateTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F] =
    new SlackObserver[F](client, translator = f(translator), threshold)

  private def publish(client: SimpleNotificationService[F], snsArn: SnsArn, msg: String): F[Unit] = {
    val req: PublishRequest.Builder = PublishRequest.builder().topicArn(snsArn.value).message(msg)
    client.publish(req.build()).attempt.void
  }

  def observe(snsArn: SnsArn): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      sns <- Stream.resource(client)
      ofm <- Stream.eval(
        F.ref[Map[ServiceId, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(e =>
          translator.filter(EventPipe.alarmLevel(threshold))
            .translate(e)
            .flatMap(_.traverse(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
        .onFinalize(ofm.terminated.flatMap(_.traverse_(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
    } yield event
}
