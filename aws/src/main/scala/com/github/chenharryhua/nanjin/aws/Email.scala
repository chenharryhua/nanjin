package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.kernel.Sync
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.model.*
import com.amazonaws.services.simpleemail.{AmazonSimpleEmailService, AmazonSimpleEmailServiceClientBuilder}

final case class EmailContent(from: String, to: List[String], subject: String, body: String)

trait Email[F[_]] {
  def send(txt: EmailContent): F[SendEmailResult]
}

object Email {
  def apply[F[_]: Sync](regions: Regions): Email[F]    = new EmailImpl(regions)
  def apply[F[_]: Sync]: Email[F]                      = apply[F](defaultRegion)
  def fake[F[_]](implicit F: Applicative[F]): Email[F] = (_: EmailContent) => F.pure(new SendEmailResult)

  final private class EmailImpl[F[_]](regions: Regions)(implicit F: Sync[F]) extends Email[F] {

    private lazy val sesClient: AmazonSimpleEmailService =
      AmazonSimpleEmailServiceClientBuilder.standard().withRegion(regions).build

    override def send(content: EmailContent): F[SendEmailResult] = {
      val request = new SendEmailRequest()
        .withDestination(new Destination().withToAddresses(content.to*))
        .withMessage(
          new Message()
            .withBody(new Body().withHtml(new Content().withCharset("UTF-8").withData(content.body)))
            .withSubject(new Content().withCharset("UTF-8").withData(content.subject)))
        .withSource(content.from)
      F.blocking(sesClient.sendEmail(request))
    }
  }
}
