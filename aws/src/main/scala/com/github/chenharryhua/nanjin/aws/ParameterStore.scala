package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import cats.{Applicative, Endo}
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ssm.model.{GetParametersRequest, GetParametersResponse}
import software.amazon.awssdk.services.ssm.{SsmClient, SsmClientBuilder}

import java.util.Base64

trait ParameterStore[F[_]] {
  def fetch(request: GetParametersRequest): F[GetParametersResponse]
  def fetch(path: ParameterStorePath): F[ParameterStoreContent]

  final def base64(path: ParameterStorePath)(implicit F: Applicative[F]): F[Array[Byte]] =
    fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))

  def updateBuilder(f: Endo[SsmClientBuilder]): ParameterStore[F]
}

object ParameterStore {

  private val name: String = "aws.ParameterStore"

  def apply[F[_]: Sync](f: Endo[SsmClientBuilder]): Resource[F, ParameterStore[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      ps <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsPS(f, logger))) {
        case (cw, quitCase) =>
          cw.shutdown(name, quitCase, logger)
      }
    } yield ps

  final private class AwsPS[F[_]](buildFrom: Endo[SsmClientBuilder], logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with ParameterStore[F] {
    private lazy val client: SsmClient = buildFrom(SsmClient.builder()).build()

    override def fetch(request: GetParametersRequest): F[GetParametersResponse] =
      F.blocking(client.getParameters(request)).onError(ex => logger.error(ex)(request.toString))

    override def fetch(path: ParameterStorePath): F[ParameterStoreContent] = {
      val request: GetParametersRequest =
        GetParametersRequest.builder().names(path.value).withDecryption(path.isSecure).build()
      F.blocking(ParameterStoreContent(client.getParameters(request).parameters.get(0).value()))
        .onError(ex => logger.error(ex)(request.toString))
    }

    override protected val closeService: F[Unit] = F.blocking(client.close())

    override def updateBuilder(f: Endo[SsmClientBuilder]): ParameterStore[F] =
      new AwsPS[F](buildFrom.andThen(f), logger)
  }
}
