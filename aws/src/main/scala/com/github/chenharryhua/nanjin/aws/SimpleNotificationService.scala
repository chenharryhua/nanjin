package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import cats.Endo
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sns.{SnsClient, SnsClientBuilder}
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

sealed trait SimpleNotificationService[F[_]] {
  def publish(request: PublishRequest): F[PublishResponse]
  def updateBuilder(f: Endo[SnsClientBuilder]): SimpleNotificationService[F]

  final def publish(f: Endo[PublishRequest.Builder]): F[PublishResponse] =
    publish(f(PublishRequest.builder()).build())
}

object SimpleNotificationService {

  private val name: String = "aws.SNS"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, SimpleNotificationService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.make(F.pure(new SimpleNotificationService[F] {
      override def publish(request: PublishRequest): F[PublishResponse] =
        logger.info(request.toString) *> F.pure(
          PublishResponse
            .builder()
            .messageId("fake.message.id")
            .sequenceNumber("fake.sequence.number")
            .build())

      override def updateBuilder(f: Endo[SnsClientBuilder]): SimpleNotificationService[F] =
        this
    }))(_ => F.unit)
  }

  def apply[F[_]: Sync](f: Endo[SnsClientBuilder]): Resource[F, SimpleNotificationService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      nr <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSNS[F](f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield nr

  final private class AwsSNS[F[_]](buildFrom: Endo[SnsClientBuilder], logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with SimpleNotificationService[F] {

    private lazy val client: SnsClient = buildFrom(SnsClient.builder).build()

    override def publish(request: PublishRequest): F[PublishResponse] =
      F.blocking(client.publish(request)).onError(ex => logger.error(ex)(request.toString))

    override protected val closeService: F[Unit] = F.blocking(client.close())

    override def updateBuilder(f: Endo[SnsClientBuilder]): SimpleNotificationService[F] =
      new AwsSNS[F](buildFrom.andThen(f), logger)
  }
}
