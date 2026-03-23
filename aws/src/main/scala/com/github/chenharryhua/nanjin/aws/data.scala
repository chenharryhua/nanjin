package com.github.chenharryhua.nanjin.aws

import cats.Show
import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.IronRefined.PlusConversion
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Codec, Decoder, Encoder}
import io.github.iltotore.iron.constraint.all.ValidURL
import io.github.iltotore.iron.constraint.string.{EndWith, Match}
import io.github.iltotore.iron.{DescribedAs, Not, RefinedType}

private type EAC = Match["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]
type Email = Email.T
object Email extends RefinedType[String, EAC] with PlusConversion[String, EAC] {
  given Show[Email] = OpaqueLift.lift[Email, String, Show]
  given Encoder[Email] = OpaqueLift.lift[Email, String, Encoder]
  given Decoder[Email] = OpaqueLift.lift[Email, String, Decoder]
}

private type IAC = Match["^arn:(aws[a-zA-Z-]*)?:iam::\\d{12}:role/[A-Za-z0-9-]+$"]
type IamArn = IamArn.T
object IamArn extends RefinedType[String, IAC] with PlusConversion[String, IAC] {
  given Show[IamArn] = OpaqueLift.lift[IamArn, String, Show]
  given Encoder[IamArn] = OpaqueLift.lift[IamArn, String, Encoder]
  given Decoder[IamArn] = OpaqueLift.lift[IamArn, String, Decoder]
}

private type SAC = Match["^arn:(aws[a-zA-Z-]*)?:sns:[A-Za-z0-9_-]+:\\d{12}:[A-Za-z0-9-]+$"]
type SnsArn = SnsArn.T
object SnsArn extends RefinedType[String, SAC] with PlusConversion[String, SAC] {
  given Show[SnsArn] = OpaqueLift.lift[SnsArn, String, Show]
  given Encoder[SnsArn] = OpaqueLift.lift[SnsArn, String, Encoder]
  given Decoder[SnsArn] = OpaqueLift.lift[SnsArn, String, Decoder]
}

private type KAC = Match["^arn:(aws[a-zA-Z-]*)?:kms:[A-Za-z0-9-]+:\\d{12}:key/[A-Za-z0-9-]+$"]
type KmsArn = KmsArn.T
object KmsArn extends RefinedType[String, KAC] with PlusConversion[String, KAC] {
  given Show[KmsArn] = OpaqueLift.lift[KmsArn, String, Show]
  given Encoder[KmsArn] = OpaqueLift.lift[KmsArn, String, Encoder]
  given Decoder[KmsArn] = OpaqueLift.lift[KmsArn, String, Decoder]
}

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Namespace
private type CWC = Match["""^[a-zA-Z0-9_.\-#:]+$"""]
type CloudWatchNs = CloudWatchNs.T
object CloudWatchNs extends RefinedType[String, CWC] with PlusConversion[String, CWC] {
  given Show[CloudWatchNs] = OpaqueLift.lift[CloudWatchNs, String, Show]
  given Encoder[CloudWatchNs] = OpaqueLift.lift[CloudWatchNs, String, Encoder]
  given Decoder[CloudWatchNs] = OpaqueLift.lift[CloudWatchNs, String, Decoder]
}

// https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
final case class ParameterStorePath(value: String, isSecure: Boolean = true) derives Codec.AsObject

final case class ParameterStoreContent(value: String) derives Codec.AsObject

/** @param from
  *   sender email address
  * @param to
  *   primary recipients (non-empty)
  * @param cc
  *   optional carbon copy recipients
  * @param bcc
  *   optional blind carbon copy recipients
  */
final case class EmailContent(
  from: Email,
  to: NonEmptyList[Email],
  subject: String,
  body: String,
  cc: List[Email] = List.empty,
  bcc: List[Email] = List.empty)

object EmailContent:
  import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
  given Encoder[EmailContent] = deriveEncoder
  given Decoder[EmailContent] = deriveDecoder
  // extends Codec.AsObject

// sqs

object SqsUrl {
  private type SC = DescribedAs[ValidURL & Not[EndWith[".fifo"]], "Standard Queue must not end with fifo"]
  type Standard = Standard.T
  object Standard extends RefinedType[String, SC] with PlusConversion[String, SC] {
    given Show[Standard] = OpaqueLift.lift[Standard, String, Show]
    given Encoder[Standard] = OpaqueLift.lift[Standard, String, Encoder]
    given Decoder[Standard] = OpaqueLift.lift[Standard, String, Decoder]
  }

  private type FC = DescribedAs[ValidURL & EndWith[".fifo"], "FIFO queue must end with .fifo"]
  type Fifo = Fifo.T
  object Fifo extends RefinedType[String, FC] with PlusConversion[String, FC] {
    given Show[Fifo] = OpaqueLift.lift[Fifo, String, Show]
    given Encoder[Fifo] = OpaqueLift.lift[Fifo, String, Encoder]
    given Decoder[Fifo] = OpaqueLift.lift[Fifo, String, Decoder]
  }
}
