package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxApplicativeError, toFunctorOps}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResponse}
import software.amazon.awssdk.services.secretsmanager.{SecretsManagerClient, SecretsManagerClientBuilder}

trait SecretsManager[F[_]] {
  def getValue(req: GetSecretValueRequest): F[GetSecretValueResponse]
  def getString(secretId: String): F[String]
  def getBinary(secretId: String): F[SdkBytes]
}

object SecretsManager {
  private val name: String = "aws.SecretsManager"

  def apply[F[_]: Sync](f: Endo[SecretsManagerClientBuilder]): Resource[F, SecretsManager[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      ps <- Resource.makeCase(logger.info(s"initialize $name").as(new SecretsManagerImpl(f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield ps

  final private class SecretsManagerImpl[F[_]](
    buildFrom: Endo[SecretsManagerClientBuilder],
    logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with SecretsManager[F] {

    private lazy val client: SecretsManagerClient = buildFrom(SecretsManagerClient.builder()).build()

    override def getValue(req: GetSecretValueRequest): F[GetSecretValueResponse] =
      F.blocking(client.getSecretValue(req)).onError(ex => logger.error(ex)(req.toString))

    override def getString(secretId: String): F[String] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).map(_.secretString())

    override def getBinary(secretId: String): F[SdkBytes] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).map(_.secretBinary())

    override protected val closeService: F[Unit] = F.blocking(client.close())
  }
}
