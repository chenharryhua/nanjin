package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.model.*
import com.amazonaws.services.simpleemail.{AmazonSimpleEmailService, AmazonSimpleEmailServiceClientBuilder}
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}

@JsonCodec
final case class EmailContent(from: String, to: List[String], subject: String, body: String)

trait SimpleEmailService[F[_]] {
  def send(txt: EmailContent): F[SendEmailResult]
}

object ses {
  private val name: String = "aws.SES"
  def apply[F[_]](regions: Regions)(implicit F: Sync[F]): Resource[F, SimpleEmailService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

    Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSES[F](regions, logger))) {
      case (cw, quitCase) =>
        val logging = quitCase match {
          case ExitCase.Succeeded  => logger.info(s"$name was closed normally")
          case ExitCase.Errored(e) => logger.warn(e)(s"$name was closed abnormally")
          case ExitCase.Canceled   => logger.info(s"$name was canceled")
        }
        logging *> cw.shutdown
    }
  }

  def apply[F[_]: Sync]: Resource[F, SimpleEmailService[F]] = apply[F](defaultRegion)

  def fake[F[_]](implicit F: Sync[F]): Resource[F, SimpleEmailService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.make(F.pure(new SimpleEmailService[F] {
      override def send(txt: EmailContent): F[SendEmailResult] =
        logger.info(txt.asJson.noSpaces) *> F.pure(new SendEmailResult)
    }))(_ => F.unit)
  }

  final private class AwsSES[F[_]](regions: Regions, logger: Logger[F])(implicit F: Sync[F])
      extends SimpleEmailService[F] with ShutdownService[F] {

    private val client: AmazonSimpleEmailService =
      AmazonSimpleEmailServiceClientBuilder.standard().withRegion(regions).build

    override def send(content: EmailContent): F[SendEmailResult] = {
      val request = new SendEmailRequest()
        .withDestination(new Destination().withToAddresses(content.to*))
        .withMessage(
          new Message()
            .withBody(new Body().withHtml(new Content().withCharset("UTF-8").withData(content.body)))
            .withSubject(new Content().withCharset("UTF-8").withData(content.subject)))
        .withSource(content.from)
      F.blocking(client.sendEmail(request)).attempt.flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r)).rethrow
    }

    override def shutdown: F[Unit] = F.blocking(client.shutdown())
  }
}
