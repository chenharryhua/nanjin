package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClientBuilder
}
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}

import java.util.Base64

sealed trait ParameterStore[F[_]] {
  def fetch(path: ParameterStorePath): F[ParameterStoreContent]

  def base64(path: ParameterStorePath): F[Array[Byte]]
}

object ParameterStore {

  def fake[F[_]](content: String)(implicit F: Applicative[F]): Resource[F, ParameterStore[F]] =
    Resource.make(F.pure(new ParameterStore[F] {

      override def fetch(path: ParameterStorePath): F[ParameterStoreContent] =
        ParameterStoreContent(content).pure

      override def base64(path: ParameterStorePath): F[Array[Byte]] =
        fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))

    }))(_ => F.unit)

  def apply[F[_]](regions: Regions)(implicit F: Sync[F]): Resource[F, ParameterStore[F]] =
    Resource.make(F.delay(new PS(regions)))(_.shutdown)

  def apply[F[_]: Async]: Resource[F, ParameterStore[F]] = apply[F](defaultRegion)

  final private class PS[F[_]](regions: Regions)(implicit F: Sync[F])
      extends ParameterStore[F] with ShutdownService[F] {
    private val client: AWSSimpleSystemsManagement =
      AWSSimpleSystemsManagementClientBuilder.standard.withRegion(regions).build

    override def fetch(path: ParameterStorePath): F[ParameterStoreContent] = {
      val req = new GetParametersRequest().withNames(path.value).withWithDecryption(path.isSecure)
      F.blocking(ParameterStoreContent(client.getParameters(req).getParameters.get(0).getValue))
    }

    override def base64(path: ParameterStorePath): F[Array[Byte]] =
      fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))

    override def shutdown: F[Unit] = F.blocking(client.shutdown())
  }
}
