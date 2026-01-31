package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}
import software.amazon.awssdk.services.sns.{SnsClient, SnsClientBuilder}

trait SimpleNotificationService[F[_]] {
  def publish(request: PublishRequest): F[PublishResponse]

  final def publish(f: Endo[PublishRequest.Builder]): F[PublishResponse] =
    publish(f(PublishRequest.builder()).build())
}

object SimpleNotificationService {

  private val name: String = "aws.SNS"

  def apply[F[_]](f: Endo[SnsClientBuilder])(implicit
    F: Async[F]): Resource[F, SimpleNotificationService[F]] =
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
