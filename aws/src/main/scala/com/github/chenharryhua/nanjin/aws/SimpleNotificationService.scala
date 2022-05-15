package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}

sealed trait SimpleNotificationService[F[_]] {
  def publish(snsArn: SnsArn, msg: String): F[PublishResult]
  def updateBuilder(f: AmazonSNSClientBuilder => AmazonSNSClientBuilder): SimpleNotificationService[F]
}

object SimpleNotificationService {

  private val name: String = "aws.SNS"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, SimpleNotificationService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.make(F.pure(new SimpleNotificationService[F] {
      override def publish(snsArn: SnsArn, msg: String): F[PublishResult] =
        logger.info(msg) *> F.pure(new PublishResult)

      override def updateBuilder(f: AmazonSNSClientBuilder => AmazonSNSClientBuilder): SimpleNotificationService[F] =
        this
    }))(_ => F.unit)
  }

  def apply[F[_]: Sync](
    f: AmazonSNSClientBuilder => AmazonSNSClientBuilder): Resource[F, SimpleNotificationService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      nr <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSNS[F](f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield nr

  final private class AwsSNS[F[_]](buildFrom: AmazonSNSClientBuilder => AmazonSNSClientBuilder, logger: Logger[F])(
    implicit F: Sync[F])
      extends ShutdownService[F] with SimpleNotificationService[F] {

    private lazy val client: AmazonSNS = buildFrom(AmazonSNSClientBuilder.standard()).build()

    override def publish(snsArn: SnsArn, msg: String): F[PublishResult] =
      F.blocking(client.publish(new PublishRequest(snsArn.value, msg)))
        .attempt
        .flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r))
        .rethrow

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def updateBuilder(f: AmazonSNSClientBuilder => AmazonSNSClientBuilder): SimpleNotificationService[F] =
      new AwsSNS[F](buildFrom.andThen(f), logger)
  }
}
