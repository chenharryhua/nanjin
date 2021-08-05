package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.model.*
import com.amazonaws.services.simpleemail.{AmazonSimpleEmailService, AmazonSimpleEmailServiceClientBuilder}

final case class EmailContent(from: String, to: List[String], subject: String, body: String)

trait Email[F[_]] {
  def send(txt: EmailContent): F[SendEmailResult]
}

object Email {
  def apply[F[_]](regions: Regions)(implicit F: Sync[F]): Resource[F, Email[F]] =
    Resource.make(F.delay(new EmailImpl[F](regions)))(_.shutdown)

  def apply[F[_]: Sync]: Resource[F, Email[F]] = apply[F](defaultRegion)

  def fake[F[_]](implicit F: Applicative[F]): Resource[F, Email[F]] =
    Resource.make(F.pure(new Email[F] {
      override def send(txt: EmailContent): F[SendEmailResult] = F.pure(new SendEmailResult)
    }))(_ => F.unit)

  final private class EmailImpl[F[_]](regions: Regions)(implicit F: Sync[F]) extends Email[F] with ShutdownService[F] {

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
      F.blocking(client.sendEmail(request))
    }

    override def shutdown: F[Unit] = F.blocking(client.shutdown())
  }
}
