package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.foldable.toFoldableOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*
import software.amazon.awssdk.services.sns.model.PublishRequest

import com.github.chenharryhua.nanjin.guard.config.ServiceId

object SlackObserver {
  def apply[F[_]: Concurrent: Clock](client: Resource[F, SimpleNotificationService[F]]): SlackObserver[F] =
    new SlackObserver[F](client, SlackTranslator[F])
}

/** Notes: slack messages `https://api.slack.com/docs/messages/builder`
  */

final class SlackObserver[F[_]: Clock](
  client: Resource[F, SimpleNotificationService[F]],
  translator: Translator[F, SlackApp])(implicit F: Concurrent[F])
    extends UpdateTranslator[F, SlackApp, SlackObserver[F]] {

  /** supporters will be notified:
    *
    * ServicePanic
    *
    * ServiceStop
    */
  def at(supporters: String): SlackObserver[F] = {
    val sp = Translator.servicePanic[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    val st = Translator.serviceStop[F, SlackApp].modify(_.map(_.prependMarkdown(supporters)))
    new SlackObserver[F](client, translator = sp.andThen(st)(translator))
  }

  override def updateTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F] =
    new SlackObserver[F](client, translator = f(translator))

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
          translator.translate(e).flatMap(_.traverse(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
        .onFinalize(ofm.terminated.flatMap(_.traverse_(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
    } yield event
}
