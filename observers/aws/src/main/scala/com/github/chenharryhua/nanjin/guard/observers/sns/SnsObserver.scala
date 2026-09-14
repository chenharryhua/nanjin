package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.Endo
import cats.effect.kernel.{Clock, Concurrent, Resource, Temporal}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.aws.{SimpleNotificationService, SnsArn}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.*
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.*
import software.amazon.awssdk.services.sns.model.PublishRequest

object SnsObserver {

  /** Create an observer that renders events as pretty-printed JSON using the default `PrettyJsonTranslator`.
    * Refine the translator with `withTranslator`.
    */
  def apply[F[_]: Temporal](client: Resource[F, SimpleNotificationService[F]]): SnsObserver[F] =
    new SnsObserver[F](client, PrettyJsonTranslator[F])
}

/** Observer that renders each event as JSON and publishes it to an SNS topic.
  *
  * The translator produces a `Json` payload, which is serialized and published to the given SNS topic. Every
  * event is published immediately (no batching). On stream finalization, a `ServiceStop` is synthesized and
  * published for each service still running. Publish failures are swallowed so one failure does not tear down
  * the observer.
  *
  * For Slack-formatted delivery, use the dedicated webhook-based Slack observer instead; this observer is a
  * generic JSON sink for any SNS subscriber.
  */
final class SnsObserver[F[_]: Clock] private (
  client: Resource[F, SimpleNotificationService[F]],
  translator: Translator[F, Json])(using F: Concurrent[F])
    extends UpdateTranslator[F, Json, SnsObserver[F]] {

  /** Transform the event-to-`Json` translator, e.g. to filter events or adjust formatting. */
  override def withTranslator(f: Endo[Translator[F, Json]]): SnsObserver[F] =
    new SnsObserver[F](client, translator = f(translator))

  // Publish one already-rendered message to the SNS topic. attempt swallows failures so a single failed
  // publish does not terminate the observer stream.
  private def publish(client: SimpleNotificationService[F], snsArn: SnsArn, msg: String): F[Unit] = {
    val req: PublishRequest.Builder = PublishRequest.builder().topicArn(snsArn.value).message(msg)
    client.publish(req.build()).attempt.void
  }

  /** Observe events, publishing each rendered JSON message to the given SNS topic. Events pass through
    * unchanged.
    *
    * @param snsArn
    *   the ARN of the SNS topic to publish to.
    */
  def observe(snsArn: SnsArn): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    for {
      sns <- Stream.resource(client)
      ofm <- Stream.eval(FinalizeMonitor[F])
      event <- es
        .evalTap(ofm.monitoring)
        .evalTap(e =>
          translator.translate(e)
            .flatMap(_.traverse(msg => publish(sns, snsArn, msg.asJson.noSpaces))))
        .onFinalize(ofm.terminated.flatMap(_.traverse_(e =>
          translator.translate(e).flatMap(_.traverse_(msg => publish(sns, snsArn, msg.asJson.noSpaces))))))
    } yield event
}
