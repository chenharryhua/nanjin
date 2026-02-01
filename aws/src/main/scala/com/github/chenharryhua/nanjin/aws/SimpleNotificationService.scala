package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}
import software.amazon.awssdk.services.sns.{SnsClient, SnsClientBuilder}

/** A simple wrapper around AWS SNS for publishing messages.
  *
  * Provides methods to publish messages using a fully constructed `PublishRequest` or a builder-style
  * function for convenience.
  *
  * Example usage:
  * {{{
  * import cats.effect.IO
  * import cats.Endo
  * import software.amazon.awssdk.services.sns.SnsClientBuilder
  * import software.amazon.awssdk.services.sns.model.PublishRequest
  *
  * val snsResource: Resource[IO, SimpleNotificationService[IO]] =
  *   SimpleNotificationService[IO](identity[SesClientBuilder])
  *
  * val message: PublishRequest = PublishRequest.builder()
  *   .topicArn("arn:aws:sns:us-east-1:123456789012:MyTopic")
  *   .message("Hello, SNS!")
  *   .build()
  *
  * val sendIO = snsResource.use(_.publish(message))
  * }}}
  */
trait SimpleNotificationService[F[_]] {

  /** Publishes a message to SNS using a fully constructed `PublishRequest`. */
  def publish(request: PublishRequest): F[PublishResponse]

  /** Publishes a message using a builder function for convenience.
    *
    * Example:
    * {{{
    * service.publish(_.topicArn("arn:aws:sns:us-east-1:123:topic").message("Hello!"))
    * }}}
    */
  final def publish(f: Endo[PublishRequest.Builder]): F[PublishResponse] =
    publish(f(PublishRequest.builder()).build())
}

object SimpleNotificationService {

  private val name: String = "aws.SNS"

  /** Creates a resource managing an SNS client and the SimpleNotificationService wrapper.
    *
    * Example usage:
    * {{{
    * import cats.effect.IO
    * import cats.Endo
    * import software.amazon.awssdk.services.sns.SnsClientBuilder
    *
    * val snsResource: Resource[IO, SimpleNotificationService[IO]] =
    *   SimpleNotificationService[IO](identity[SesClientBuilder])
    *
    * val sendIO = snsResource.use(_.publish(_.topicArn("arn:aws:sns:us-east-1:123:topic").message("Hello!")))
    * }}}
    */
  def apply[F[_]](f: Endo[SnsClientBuilder])(implicit
    F: Async[F]
  ): Resource[F, SimpleNotificationService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.makeCase(logger.info(s"initialize $name").map(_ => f(SnsClient.builder).build())) {
        case (cw, quitCase) => shutdown(name, quitCase, logger)(F.blocking(cw.close()))
      }
    } yield new AwsSNS[F](client, logger)

  final private class AwsSNS[F[_]](client: SnsClient, logger: Logger[F])(implicit F: Sync[F])
      extends SimpleNotificationService[F] {

    override def publish(request: PublishRequest): F[PublishResponse] =
      blockingF(client.publish(request), request.toString, logger)
  }
}
