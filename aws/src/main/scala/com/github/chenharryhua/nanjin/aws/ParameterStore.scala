package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.Sync
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClientBuilder
}

import java.util.Base64

final case class ParameterStorePath(value: String, isSecure: Boolean = true)

final case class ParameterStoreContent(value: String) {
  val base64: Array[Byte] = Base64.getDecoder.decode(value.getBytes)
}

trait ParameterStore[F[_]] {
  def fetch(path: ParameterStorePath)(implicit F: Sync[F]): F[ParameterStoreContent]
}

object ParameterStore {

  def fake[F[_]: Applicative](content: String): ParameterStore[F] =
    new ParameterStore[F] {

      override def fetch(path: ParameterStorePath)(implicit F: Sync[F]): F[ParameterStoreContent] =
        F.pure(ParameterStoreContent(content))
    }

  def apply[F[_]](regions: Regions = Regions.AP_SOUTHEAST_2): ParameterStore[F] =
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
