package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import cats.Endo
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed trait SimpleNotificationService[F[_]] {
  def publish(request: PublishRequest): F[PublishResult]
  def updateBuilder(f: Endo[AmazonSNSClientBuilder]): SimpleNotificationService[F]
}

object SimpleNotificationService {

  private val name: String = "aws.SNS"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, SimpleNotificationService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.make(F.pure(new SimpleNotificationService[F] {
      override def publish(request: PublishRequest): F[PublishResult] =
        logger.info(request.toString) *> F.pure(new PublishResult)

      override def updateBuilder(f: Endo[AmazonSNSClientBuilder]): SimpleNotificationService[F] =
        this
    }))(_ => F.unit)
  }

  def apply[F[_]: Sync](f: Endo[AmazonSNSClientBuilder]): Resource[F, SimpleNotificationService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      nr <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSNS[F](f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield nr

  final private class AwsSNS[F[_]](buildFrom: Endo[AmazonSNSClientBuilder], logger: Logger[F])(implicit
    F: Sync[F])
      extends ShutdownService[F] with SimpleNotificationService[F] {

    private lazy val client: AmazonSNS = buildFrom(AmazonSNSClientBuilder.standard()).build()

    override def publish(request: PublishRequest): F[PublishResult] =
      F.blocking(client.publish(request)).onError(ex => logger.error(ex)(name))

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def updateBuilder(f: Endo[AmazonSNSClientBuilder]): SimpleNotificationService[F] =
      new AwsSNS[F](buildFrom.andThen(f), logger)
  }
}
