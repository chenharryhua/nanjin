package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}

sealed trait SimpleNotificationService[F[_]] {
  def publish(msg: => String): F[PublishResult]
}

object sns {

  private val name: String = "aws.SNS"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, SimpleNotificationService[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.make(F.pure(new SimpleNotificationService[F] {
      override def publish(msg: => String): F[PublishResult] =
        logger.info(msg) *> F.pure(new PublishResult())
    }))(_ => F.unit)
  }

  def apply[F[_]](topic: SnsArn, region: Regions)(implicit F: Sync[F]): Resource[F, SimpleNotificationService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      nr <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsSNS[F](topic, region, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield nr

  def apply[F[_]: Sync](topic: SnsArn): Resource[F, SimpleNotificationService[F]] = apply[F](topic, defaultRegion)

  final private class AwsSNS[F[_]](topic: SnsArn, region: Regions, logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with SimpleNotificationService[F] {
    private val client: AmazonSNS = AmazonSNSClientBuilder.standard().withRegion(region).build()

    override def publish(msg: => String): F[PublishResult] =
      F.blocking(client.publish(new PublishRequest(topic.value, msg)))
        .attempt
        .flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r))
        .rethrow

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())
  }
}
