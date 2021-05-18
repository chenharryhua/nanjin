package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClientBuilder
}
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}

import java.util.Base64

trait ParameterStore[F[_]] {
  def fetch(path: ParameterStorePath)(implicit F: Sync[F]): F[ParameterStoreContent]

  final def base64(path: ParameterStorePath)(implicit F: Sync[F]): F[Array[Byte]] =
    fetch(path).map(c => Base64.getDecoder.decode(c.value.getBytes))
}

object ParameterStore {

  def fake[F[_]: Applicative](content: String): ParameterStore[F] =
    new ParameterStore[F] {

      override def fetch(path: ParameterStorePath)(implicit F: Sync[F]): F[ParameterStoreContent] =
        F.pure(ParameterStoreContent(content))

    }

  def apply[F[_]](regions: Regions): ParameterStore[F] =
    new ParameterStore[F] {

      private lazy val ssmClient: AWSSimpleSystemsManagement =
        AWSSimpleSystemsManagementClientBuilder.standard.withRegion(regions).build

      override def fetch(path: ParameterStorePath)(implicit F: Sync[F]): F[ParameterStoreContent] = {
        val req =
          new GetParametersRequest().withNames(path.value).withWithDecryption(path.isSecure)
        F.blocking(ParameterStoreContent(ssmClient.getParameters(req).getParameters.get(0).getValue))
      }
    }
}
