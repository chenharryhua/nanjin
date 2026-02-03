package com.github.chenharryhua.nanjin.aws

import cats.{Applicative, Endo}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ssm.{SsmClient, SsmClientBuilder}
import software.amazon.awssdk.services.ssm.model.{GetParametersRequest, GetParametersResponse}

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

  /** Fetch a parameter by a typed path, returning its content.
    *
    * Usage example:
    * {{{
    * val path = ParameterStorePath("my/secure/parameter", isSecure = true)
    * val contentF: F[ParameterStoreContent] = parameterStore.fetch(path)
    * contentF.map(_.value) // Access the actual parameter value
    * }}}
    */
  def fetch(path: ParameterStorePath): F[ParameterStoreContent]

  /** Decode the parameter value as Base64.
    *
    * Usage example:
    * {{{
    * val path = ParameterStorePath("my/secure/parameter", isSecure = true)
    * val bytesF: F[Array[Byte]] = parameterStore.base64(path)
    * }}}
    */
  final def base64(path: ParameterStorePath)(implicit F: Applicative[F]): F[Array[Byte]] =
    fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))
}

object ParameterStore {

  private val name = "aws.ParameterStore"

  def apply[F[_]](f: Endo[SsmClientBuilder])(implicit F: Async[F]): Resource[F, ParameterStore[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.makeCase(logger.info(s"initialize $name").as(f(SsmClient.builder()).build())) {
        case (client, quitCase) =>
          shutdown(name, quitCase, logger)(F.blocking(client.close()))
      }
    } yield new AwsPS[F](client, logger)

  final private class AwsPS[F[_]](client: SsmClient, logger: Logger[F])(implicit F: Async[F])
      extends ParameterStore[F] {

    override def fetch(request: GetParametersRequest): F[GetParametersResponse] =
      blockingF(client.getParameters(request), request.toString, logger)

    override def fetch(path: ParameterStorePath): F[ParameterStoreContent] = {
      val request = GetParametersRequest.builder().names(path.value).withDecryption(path.isSecure).build()

      blockingF(
        client
          .getParameters(request)
          .parameters
          .asScala
          .headOption
          .map(p => ParameterStoreContent(p.value())),
        request.toString,
        logger
      ).flatMap(psc => F.fromOption(psc, new NoSuchElementException(s"No parameter found at ${path.value}")))
    }
  }
}
