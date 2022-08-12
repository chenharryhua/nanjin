package com.github.chenharryhua.nanjin.aws

import cats.{Applicative, Endo}
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClientBuilder
}
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.Base64

sealed trait ParameterStore[F[_]] {
  def fetch(path: ParameterStorePath): F[ParameterStoreContent]

  final def base64(path: ParameterStorePath)(implicit F: Applicative[F]): F[Array[Byte]] =
    fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))

  def updateBuilder(f: Endo[AWSSimpleSystemsManagementClientBuilder]): ParameterStore[F]
}

object ParameterStore {

  private val name: String = "aws.ParameterStore"

  def fake[F[_]](content: String)(implicit F: Applicative[F]): Resource[F, ParameterStore[F]] =
    Resource.make(F.pure(new ParameterStore[F] {

      override def fetch(path: ParameterStorePath): F[ParameterStoreContent] =
        ParameterStoreContent(content).pure

      override def updateBuilder(f: Endo[AWSSimpleSystemsManagementClientBuilder]): ParameterStore[F] = this
    }))(_ => F.unit)

  def apply[F[_]: Sync](f: Endo[AWSSimpleSystemsManagementClientBuilder]): Resource[F, ParameterStore[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      ps <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsPS(f, logger))) {
        case (cw, quitCase) =>
          cw.shutdown(name, quitCase, logger)
      }
    } yield ps

  final private class AwsPS[F[_]](
    buildFrom: Endo[AWSSimpleSystemsManagementClientBuilder],
    logger: Logger[F])(implicit F: Sync[F])
      extends ShutdownService[F] with ParameterStore[F] {
    private lazy val client: AWSSimpleSystemsManagement =
      buildFrom(AWSSimpleSystemsManagementClientBuilder.standard()).build()

    override def fetch(path: ParameterStorePath): F[ParameterStoreContent] = {
      val req = new GetParametersRequest().withNames(path.value).withWithDecryption(path.isSecure)
      F.blocking(ParameterStoreContent(client.getParameters(req).getParameters.get(0).getValue))
        .onError(ex => logger.error(ex)(name))
    }

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def updateBuilder(f: Endo[AWSSimpleSystemsManagementClientBuilder]): ParameterStore[F] =
      new AwsPS[F](buildFrom.andThen(f), logger)
  }
}
