package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ssm.model.{GetParametersRequest, GetParametersResponse}
import software.amazon.awssdk.services.ssm.{SsmClient, SsmClientBuilder}

import java.util.Base64
import scala.jdk.CollectionConverters.*

/** Abstraction over AWS Systems Manager Parameter Store.
  *
  * Provides safe access to parameters with logging and blocking protection. Does not enforce retries or
  * timeouts; these are the responsibility of the caller.
  */
trait ParameterStore[F[_]] {

  /** Fetch parameters using a raw GetParametersRequest.
    *
    * Usage example:
    * {{{
    * val request = GetParametersRequest.builder()
    *   .names("my-parameter")
    *   .withDecryption(true)
    *   .build()
    *
    * val resultF: F[GetParametersResponse] = parameterStore.fetch(request)
    * }}}
    */
  def fetch(request: GetParametersRequest): F[GetParametersResponse]
  final def fetch(f: Endo[GetParametersRequest.Builder]): F[GetParametersResponse] =
    fetch(f(GetParametersRequest.builder()).build())

  /** Fetch a parameter by its path, returning its content. `isSecure` (default `true`) requests decryption
    * for SecureString parameters.
    *
    * Usage example:
    * {{{
    * val contentF: F[ParameterStoreContent] = parameterStore.fetch("my/secure/parameter")
    * contentF.map(_.value) // Access the actual parameter value
    * }}}
    */
  def fetch(path: String, isSecure: Boolean = true): F[ParameterStoreContent]

  /** Decode the parameter value as Base64. `isSecure` (default `true`) requests decryption. */
  def base64(path: String, isSecure: Boolean = true): F[Array[Byte]]
}

object ParameterStore {

  private val name = "aws.ParameterStore"

  def apply[F[_]](f: Endo[SsmClientBuilder])(using F: Sync[F]): Resource[F, ParameterStore[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name") >> F.blocking(f(SsmClient.builder()).build())) {
        client => shutdown(name, logger)(client.close())
      }
    } yield new ParameterStoreImpl[F](client, logger)

  final private class ParameterStoreImpl[F[_]](client: SsmClient, logger: Logger[F])(using F: Sync[F])
      extends ParameterStore[F] {

    override def fetch(request: GetParametersRequest): F[GetParametersResponse] =
      blockingF(client.getParameters(request), request.toString, logger)

    override def base64(path: String, isSecure: Boolean): F[Array[Byte]] =
      fetch(path, isSecure).map(c => Base64.getDecoder.decode(c.value.getBytes))

    override def fetch(path: String, isSecure: Boolean): F[ParameterStoreContent] = {
      val request = GetParametersRequest.builder().names(path).withDecryption(isSecure).build()
      blockingF(
        client
          .getParameters(request)
          .parameters
          .asScala
          .headOption
          .map(p => ParameterStoreContent(p.value())),
        request.toString,
        logger
      ).flatMap(psc => F.fromOption(psc, new NoSuchElementException(s"No parameter found at $path")))
    }
  }
}
