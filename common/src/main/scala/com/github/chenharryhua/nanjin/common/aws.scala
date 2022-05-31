package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.data.NonEmptyList
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.auto.*
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.{MaxSize, NonEmpty}
import eu.timepit.refined.predicates.all.{And, Not}
import eu.timepit.refined.string.{EndsWith, MatchesRegex, Url}
import io.circe.generic.JsonCodec

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

  @JsonCodec
  final case class S3Path(bucket: String, key: String) {
    val s3: String  = s"${S3Protocols.S3.value}://$bucket/$key"
    val s3a: String = s"${S3Protocols.S3A.value}://$bucket/$key"
  }

  object S3Path {
    implicit final val showS3Path: Show[S3Path] = cats.derived.semiauto.show[S3Path]
  }

  // https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
  final case class ParameterStorePath(value: String, isSecure: Boolean = true)

  final case class ParameterStoreContent(value: String)

  @JsonCodec
  final case class EmailContent(from: String, to: NonEmptyList[String], subject: String, body: String)

  object EmailContent {
    implicit val showEmailContent: Show[EmailContent] = cats.derived.semiauto.show[EmailContent]
  }

  sealed trait SqsUrl {
    def queueUrl: String
  }
  object SqsUrl {

    type StandardSqsUrl = String Refined And[Url, Not[EndsWith[".fifo"]]]
    type FifoSqsUrl     = String Refined And[Url, EndsWith[".fifo"]]
    type MessageGroupID = String Refined And[NonEmpty, MaxSize[128]]
    final case class Standard(value: StandardSqsUrl) extends SqsUrl {
      override val queueUrl: String = value.value
    }
    final case class Fifo(value: FifoSqsUrl, messageGroupId: MessageGroupID) extends SqsUrl {
      override val queueUrl: String                      = value.value
      def withMessageGroupId(mgId: MessageGroupID): Fifo = copy(messageGroupId = mgId)
    }

    object Fifo {
      def apply(fifo: FifoSqsUrl): Fifo = Fifo(fifo, "nj.sqs.fifo.default.group")
    }
  }
}
