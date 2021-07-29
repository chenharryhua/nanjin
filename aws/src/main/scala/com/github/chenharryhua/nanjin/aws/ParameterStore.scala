package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.kernel.Sync
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

  def fake[F[_]: Applicative](content: String): ParameterStore[F] =
    new ParameterStore[F] {

      override def fetch(path: ParameterStorePath): F[ParameterStoreContent] =
        ParameterStoreContent(content).pure

      override def base64(path: ParameterStorePath): F[Array[Byte]] =
        fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))

    }

  def apply[F[_]](regions: Regions)(implicit F: Sync[F]): ParameterStore[F] =
    new ParameterStore[F] {

      private lazy val ssmClient: AWSSimpleSystemsManagement =
        AWSSimpleSystemsManagementClientBuilder.standard.withRegion(regions).build

      override def fetch(path: ParameterStorePath): F[ParameterStoreContent] = {
        val req =
          new GetParametersRequest().withNames(path.value).withWithDecryption(path.isSecure)
        F.blocking(ParameterStoreContent(ssmClient.getParameters(req).getParameters.get(0).getValue))
      }

      override def base64(path: ParameterStorePath): F[Array[Byte]] =
        fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))
    }

  def apply[F[_]: Sync]: ParameterStore[F] = apply[F](defaultRegion)
}
