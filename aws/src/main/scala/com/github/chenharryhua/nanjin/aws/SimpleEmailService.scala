package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ses.{SesClient, SesClientBuilder}
import software.amazon.awssdk.services.ses.model.*

import scala.jdk.CollectionConverters.*

/** A simple wrapper around AWS SES for sending emails.
  *
  * Provides basic methods for sending both structured and raw emails, along with convenience methods for
  * builder-style usage and sending emails from an `EmailContent` instance.
  *
  * Example usage:
  * {{{
  * import cats.effect.IO
  * import cats.Endo
  * import com.github.chenharryhua.nanjin.common.aws.EmailContent
  * import software.amazon.awssdk.services.ses.SesClientBuilder
  *
  * val ses: Resource[IO, SimpleEmailService[IO]] =
  *   SimpleEmailService[IO](identity[SesClientBuilder])
  *
  * val email = EmailContent(
  *   from = "sender@example.com",
  *   to = List("recipient@example.com"),
  *   cc = Nil,
  *   bcc = Nil,
  *   subject = "Hello",
  *   body = "<h1>Hello world</h1>"
  * )
  *
  * val sendIO: IO[SendEmailResponse] = ses.use(_.send(email))
  * }}}
  */
trait SimpleEmailService[F[_]] {

  /** Sends a structured email using AWS SES. */
  def send(req: SendEmailRequest): F[SendEmailResponse]

  /** Sends a raw email using AWS SES. */
  def send(req: SendRawEmailRequest): F[SendRawEmailResponse]

  /** Sends an email using a builder function for convenience. */
  final def send(f: Endo[SendEmailRequest.Builder]): F[SendEmailResponse] =
    send(f(SendEmailRequest.builder()).build())

  /** Sends an email from an `EmailContent` instance. */
  final def send(content: EmailContent): F[SendEmailResponse] =
    send(toRequest(content))

  // Private helper to convert EmailContent to SendEmailRequest
  private def toRequest(content: EmailContent): SendEmailRequest =
    SendEmailRequest
      .builder()
      .source(content.from.value)
      .destination(
        Destination
          .builder()
          .toAddresses(content.to.map(_.value).distinct.toList.asJava)
          .ccAddresses(content.cc.map(_.value).distinct.asJava)
          .bccAddresses(content.bcc.map(_.value).distinct.asJava)
          .build()
      )
      .message(
        Message
          .builder()
          .subject(Content.builder().charset("UTF-8").data(content.subject).build())
          .body(
            Body.builder().html(Content.builder().charset("UTF-8").data(content.body).build()).build()
          )
          .build()
      )
      .build()
}

object SimpleEmailService {

  private val name: String = "aws.SES"

  /** Creates a resource managing an SES client and the SimpleEmailService wrapper.
    *
    * Example usage:
    * {{{
    * val ses: Resource[IO, SimpleEmailService[IO]] =
    *   SimpleEmailService[IO](identity[SesClientBuilder])
    * }}}
    */
  def apply[F[_]](f: Endo[SesClientBuilder])(implicit F: Async[F]): Resource[F, SimpleEmailService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name").as(f(SesClient.builder()).build())) { client =>
        shutdown(name, logger)(F.blocking(client.close()))
      }
    } yield new AwsSES[F](client, logger)

  final private class AwsSES[F[_]](
    client: SesClient,
    logger: Logger[F]
  )(implicit F: Sync[F])
      extends SimpleEmailService[F] {

    override def send(request: SendEmailRequest): F[SendEmailResponse] =
      blockingF(client.sendEmail(request), request.toString, logger)

    override def send(req: SendRawEmailRequest): F[SendRawEmailResponse] =
      blockingF(client.sendRawEmail(req), req.toString, logger)
  }
}
