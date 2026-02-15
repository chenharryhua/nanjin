package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.functor.toFunctorOps
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.secretsmanager.{SecretsManagerClient, SecretsManagerClientBuilder}
import software.amazon.awssdk.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResponse}

/** Provides access to AWS Secrets Manager. */
trait SecretsManager[F[_]] {

  /** Fetch the raw secret value response from AWS.
    *
    * Example:
    * {{{
    * val secretValue: F[GetSecretValueResponse] = secretsManager.getValue(
    *   GetSecretValueRequest.builder().secretId("my-secret").build()
    * )
    * }}}
    */
  def getValue(req: GetSecretValueRequest): F[GetSecretValueResponse]

  /** Fetch a secret as a UTF-8 string.
    *
    * Example:
    * {{{
    * val secret: F[String] = secretsManager.getString("my-secret")
    * }}}
    */
  def getString(secretId: String): F[String]

  /** Fetch a secret as raw binary.
    *
    * Example:
    * {{{
    * val secretBinary: F[SdkBytes] = secretsManager.getBinary("my-binary-secret")
    * }}}
    */
  def getBinary(secretId: String): F[SdkBytes]
}

object SecretsManager {

  private val name = "aws.SecretsManager"

  def apply[F[_]](f: Endo[SecretsManagerClientBuilder])(implicit
    F: Async[F]): Resource[F, SecretsManager[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name").as(f(SecretsManagerClient.builder()).build())) {
        sm => shutdown(name, logger)(F.blocking(sm.close()))
      }
    } yield new SecretsManagerImpl(client, logger)

  final private class SecretsManagerImpl[F[_]](
    client: SecretsManagerClient,
    logger: Logger[F]
  )(implicit F: Sync[F])
      extends SecretsManager[F] {

    override def getValue(req: GetSecretValueRequest): F[GetSecretValueResponse] =
      blockingF(client.getSecretValue(req), req.toString, logger)

    override def getString(secretId: String): F[String] =
      blockingF(
        client.getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build()).secretString(),
        secretId,
        logger
      )

    override def getBinary(secretId: String): F[SdkBytes] =
      blockingF(
        client.getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build()).secretBinary(),
        secretId,
        logger
      )
  }
}
