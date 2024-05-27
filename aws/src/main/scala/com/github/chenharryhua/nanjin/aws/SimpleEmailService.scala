package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ses.model.*
import software.amazon.awssdk.services.ses.{SesClient, SesClientBuilder}

import scala.jdk.CollectionConverters.*

trait SimpleEmailService[F[_]] {
  def send(req: SendEmailRequest): F[SendEmailResponse]
  def send(req: SendRawEmailRequest): F[SendRawEmailResponse]
  def updateBuilder(f: Endo[SesClientBuilder]): SimpleEmailService[F]

  final def send(f: Endo[SendEmailRequest.Builder]): F[SendEmailResponse] =
    send(f(SendEmailRequest.builder()).build())
  final def send(content: EmailContent): F[SendEmailResponse] =
    send(
      SendEmailRequest
        .builder()
        .source(content.from.value)
        .destination(
          Destination
            .builder()
            .toAddresses(content.to.map(_.value).distinct.toList.asJava)
            .ccAddresses(content.cc.map(_.value).distinct.asJava)
            .bccAddresses(content.bcc.map(_.value).distinct.asJava)
            .build())
        .message(
          Message
            .builder()
            .body(Body.builder().html(Content.builder().charset("UTF-8").data(content.body).build()).build())
            .subject(Content.builder().charset("UTF-8").data(content.subject).build())
            .build())
        .build())
}

object SimpleEmailService {

  private val name: String = "aws.SES"

  def apply[F[_]: Sync](f: Endo[SesClientBuilder]): Resource[F, SimpleEmailService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      er <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSES[F](f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield er

  final private class AwsSES[F[_]](buildFrom: Endo[SesClientBuilder], logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with SimpleEmailService[F] {

    private lazy val client: SesClient = buildFrom(SesClient.builder()).build()

    override def send(request: SendEmailRequest): F[SendEmailResponse] =
      F.blocking(client.sendEmail(request)).onError(ex => logger.error(ex)(request.toString))

    override protected val closeService: F[Unit] = F.blocking(client.close())

    override def updateBuilder(f: Endo[SesClientBuilder]): SimpleEmailService[F] =
      new AwsSES[F](buildFrom.andThen(f), logger)

    override def send(req: SendRawEmailRequest): F[SendRawEmailResponse] =
      F.blocking(client.sendRawEmail(req)).onError(ex => logger.error(ex)(req.toString))
  }
}
