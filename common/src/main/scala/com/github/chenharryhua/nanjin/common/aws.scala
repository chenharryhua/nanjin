package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.data.NonEmptyList
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.auto.*
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.{MaxSize, NonEmpty}
import eu.timepit.refined.numeric
import eu.timepit.refined.predicates.all.{And, Not}
import eu.timepit.refined.string.{EndsWith, MatchesRegex, Url}
import io.circe.generic.JsonCodec
import io.circe.refined.*
import io.circe.{Encoder, Json}

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
  @JsonCodec
  final case class ParameterStorePath(value: String, isSecure: Boolean = true)
  object ParameterStorePath {
    implicit val showParameterStorePath: Show[ParameterStorePath] =
      cats.derived.semiauto.show[ParameterStorePath]
  }

  @JsonCodec
  final case class ParameterStoreContent(value: String)
  object ParameterStoreContent {
    implicit val showParameterStoreContent: Show[ParameterStoreContent] =
      cats.derived.semiauto.show[ParameterStoreContent]
  }

  @JsonCodec
  final case class EmailContent(from: String, to: NonEmptyList[String], subject: String, body: String)

  object EmailContent {
    implicit val showEmailContent: Show[EmailContent] = cats.derived.semiauto.show[EmailContent]
  }

  // sqs

  object SqsUrl {
    type Standard = String Refined And[Url, Not[EndsWith[".fifo"]]]
    object Standard extends RefinedTypeOps[Standard, String] with CatsRefinedTypeOpsSyntax

    type Fifo = String Refined And[Url, EndsWith[".fifo"]]
    object Fifo extends RefinedTypeOps[Fifo, String] with CatsRefinedTypeOpsSyntax
  }

  @JsonCodec
  sealed trait SqsConfig {
    def queueUrl: String

    def maxNumberOfMessages: SqsConfig.MaxNumberOfMessages
    def withMaxNumberOfMessages(num: SqsConfig.MaxNumberOfMessages): SqsConfig

    def waitTimeSeconds: SqsConfig.WaitTimeSeconds
    def withWaitTimeSeconds(seconds: SqsConfig.WaitTimeSeconds): SqsConfig

    def visibilityTimeout: SqsConfig.VisibilityTimeout
    def withVisibilityTimeout(seconds: SqsConfig.VisibilityTimeout): SqsConfig

    final def asJson: Json = Encoder[SqsConfig].apply(this)
  }

  object SqsConfig {
    def apply(url: SqsUrl.Standard): Standard = Standard(url)
    def apply(url: SqsUrl.Fifo): Fifo         = Fifo(url)

    type MessageGroupID      = String Refined And[NonEmpty, MaxSize[128]]
    type MaxNumberOfMessages = Int Refined numeric.Interval.Closed[1, 10]
    type WaitTimeSeconds     = Int Refined numeric.Interval.Closed[0, 20]
    type VisibilityTimeout   = Int Refined numeric.Interval.Closed[0, 43200] // 12 hours

    private val defaultMaxNumberOfMessages: MaxNumberOfMessages = 10 // 10 messages in one fetch
    private val defaultWaitTimeSeconds: WaitTimeSeconds         = 20 // 20 seconds
    private val defaultVisibilityTimeout: VisibilityTimeout     = 30 // 30 seconds

    final case class Standard(
      value: SqsUrl.Standard,
      maxNumberOfMessages: MaxNumberOfMessages,
      waitTimeSeconds: WaitTimeSeconds,
      visibilityTimeout: VisibilityTimeout)
        extends SqsConfig {
      override val queueUrl: String = value.value

      override def withMaxNumberOfMessages(num: MaxNumberOfMessages): Standard =
        copy(maxNumberOfMessages = num)

      override def withWaitTimeSeconds(seconds: WaitTimeSeconds): Standard =
        copy(waitTimeSeconds = seconds)

      override def withVisibilityTimeout(seconds: VisibilityTimeout): Standard =
        copy(visibilityTimeout = seconds)
    }

    object Standard {
      def apply(standard: SqsUrl.Standard): Standard =
        Standard(
          value = standard,
          maxNumberOfMessages = defaultMaxNumberOfMessages,
          waitTimeSeconds = defaultWaitTimeSeconds,
          visibilityTimeout = defaultVisibilityTimeout)
    }

    final case class Fifo(
      value: SqsUrl.Fifo,
      maxNumberOfMessages: MaxNumberOfMessages,
      waitTimeSeconds: WaitTimeSeconds,
      visibilityTimeout: VisibilityTimeout,
      messageGroupId: MessageGroupID)
        extends SqsConfig {
      override val queueUrl: String = value.value

      def withMessageGroupId(mgId: MessageGroupID): Fifo =
        copy(messageGroupId = mgId)

      override def withMaxNumberOfMessages(num: MaxNumberOfMessages): Fifo =
        copy(maxNumberOfMessages = num)

      override def withWaitTimeSeconds(seconds: WaitTimeSeconds): Fifo =
        copy(waitTimeSeconds = seconds)

      override def withVisibilityTimeout(seconds: VisibilityTimeout): Fifo =
        copy(visibilityTimeout = seconds)
    }

    object Fifo {
      def apply(fifo: SqsUrl.Fifo): Fifo =
        Fifo(
          value = fifo,
          maxNumberOfMessages = defaultMaxNumberOfMessages,
          waitTimeSeconds = defaultWaitTimeSeconds,
          visibilityTimeout = defaultVisibilityTimeout,
          messageGroupId = "nj.sqs.fifo.default.group"
        )
    }
  }
}
