package com.github.chenharryhua.nanjin.common

import cats.data.NonEmptyList
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.*
import eu.timepit.refined.predicates.all.{And, Not}
import eu.timepit.refined.string.{EndsWith, MatchesRegex, Url}
import io.circe.generic.JsonCodec
import io.circe.refined.*

object aws {
  type IamArn = String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:iam::\\d{12}:role/[A-Za-z0-9-]+$"]

  object IamArn extends RefinedTypeOps[IamArn, String] with CatsRefinedTypeOpsSyntax

  type SnsArn = String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:sns:[A-Za-z0-9_-]+:\\d{12}:[A-Za-z0-9-]+$"]

  object SnsArn extends RefinedTypeOps[SnsArn, String] with CatsRefinedTypeOpsSyntax

  type KmsArn =
    String Refined MatchesRegex["^arn:(aws[a-zA-Z-]*)?:kms:[A-Za-z0-9-]+:\\d{12}:key/[A-Za-z0-9-]+$"]

  object KmsArn extends RefinedTypeOps[KmsArn, String] with CatsRefinedTypeOpsSyntax

  // https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Namespace
  type CloudWatchNamespace = String Refined MatchesRegex["""^[a-zA-Z0-9_.\-#:]+$"""]

  object CloudWatchNamespace extends RefinedTypeOps[CloudWatchNamespace, String] with CatsRefinedTypeOpsSyntax

  // https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
  @JsonCodec
  final case class ParameterStorePath(value: String, isSecure: Boolean = true)

  @JsonCodec
  final case class ParameterStoreContent(value: String)

  /** @param from
    *   sender email address
    * @param to
    *   primary recipients (non-empty)
    * @param cc
    *   optional carbon copy recipients
    * @param bcc
    *   optional blind carbon copy recipients
    */
  @JsonCodec
  final case class EmailContent(
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String,
    body: String,
    cc: List[EmailAddr] = List.empty,
    bcc: List[EmailAddr] = List.empty)

  // sqs

  object SqsUrl {
    type Standard = String Refined And[Url, Not[EndsWith[".fifo"]]]

    object Standard extends RefinedTypeOps[Standard, String] with CatsRefinedTypeOpsSyntax

    type Fifo = String Refined And[Url, EndsWith[".fifo"]]

    object Fifo extends RefinedTypeOps[Fifo, String] with CatsRefinedTypeOpsSyntax
  }
}
