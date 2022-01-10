package com.github.chenharryhua.nanjin.common

import cats.Show
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.{MatchesRegex, Url}
import io.circe.generic.JsonCodec

object aws {
  type SqsUrl = String Refined Url
  object SqsUrl extends RefinedTypeOps[SqsUrl, String] with CatsRefinedTypeOpsSyntax

  type IamArn =
    String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:iam::\\d{12}:role/[A-Za-z0-9-]+$"]
  object IamArn extends RefinedTypeOps[IamArn, String] with CatsRefinedTypeOpsSyntax

  type SnsArn =
    String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:sns:[A-Za-z0-9_-]+:\\d{12}:[A-Za-z0-9-]+$"]
  object SnsArn extends RefinedTypeOps[SnsArn, String] with CatsRefinedTypeOpsSyntax

  type KmsArn =
    String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:kms:[A-Za-z0-9-]+:\\d{12}:key/[A-Za-z0-9-]+$"]
  object KmsArn extends RefinedTypeOps[KmsArn, String] with CatsRefinedTypeOpsSyntax

  @JsonCodec
  final case class S3Path(bucket: String, key: String) {
    val s3: String  = s"${S3Protocols.S3.value}://$bucket/$key"
    val s3a: String = s"${S3Protocols.S3A.value}://$bucket/$key"
  }

  object S3Path {
    implicit val showS3Path: Show[S3Path] = cats.derived.semiauto.show[S3Path]
  }

  // https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
  final case class ParameterStorePath(value: String, isSecure: Boolean = true)

  final case class ParameterStoreContent(value: String)
}
