package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource, Temporal}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.aws.{SimpleNotificationService, SnsArn}
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.*
import fs2.{Pipe, Stream}
import io.circe.syntax.*
import software.amazon.awssdk.services.sns.model.PublishRequest

object SlackObserver {

  /** Create an observer that renders events as Slack messages using the default `SlackTranslator`. Refine the
    * translator with `withTranslator`.
    */
  def apply[F[_]: Temporal](client: Resource[F, SimpleNotificationService[F]]): SlackObserver[F] =
    new SlackObserver[F](client, SlackTranslator[F])
}

/** Observer that renders each event as a Slack message and publishes it to an SNS topic.
  *
  * The translator produces a `SlackApp` (Block Kit payload), which is serialized to JSON and published to the
  * given SNS topic; an SNS-to-Slack subscription then delivers it. Every event is published immediately (no
  * batching). On stream finalization, a `ServiceStop` is synthesized and published for each service still
  * running. Publish failures are swallowed so one failure does not tear down the observer.
  *
  * Block Kit layouts can be previewed at `https://app.slack.com/block-kit-builder`.
  */
final class SlackObserver[F[_]: Clock](
  client: Resource[F, SimpleNotificationService[F]],
  translator: Translator[F, SlackApp])(using F: Concurrent[F])
    extends UpdateTranslator[F, SlackApp, SlackObserver[F]] {

  /** Transform the event-to-`SlackApp` translator, e.g. to filter events or adjust formatting. */
  override def withTranslator(f: Endo[Translator[F, SlackApp]]): SlackObserver[F] =
    new SlackObserver[F](client, translator = f(translator))

  // Publish one already-rendered message to the SNS topic. attempt swallows failures so a single failed
  // publish does not terminate the observer stream.
  private def publish(client: SimpleNotificationService[F], snsArn: SnsArn, msg: String): F[Unit] = {
    val req: PublishRequest.Builder = PublishRequest.builder().topicArn(snsArn.value).message(msg)
    client.publish(req.build()).attempt.void
  }

  /** Observe events, publishing each rendered Slack message to the given SNS topic. Events pass through
    * unchanged.
    *
    * @param snsArn
    *   the ARN of the SNS topic subscribed to Slack.
    */
  def observe(snsArn: SnsArn): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      sns <- Stream.resource(client)
      ofm <- Stream.eval(
        F.ref[Map[ServiceId, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(e =>
          translator.translate(e)
            .flatMap(_.traverse(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
        .onFinalize(ofm.terminated.flatMap(_.traverse_(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
    } yield event
}
