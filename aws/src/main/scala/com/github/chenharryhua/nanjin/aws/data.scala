package com.github.chenharryhua.nanjin.aws

import cats.data.NonEmptyList
import io.circe.Codec
import io.github.iltotore.iron.constraint.all.ValidURL
import io.github.iltotore.iron.constraint.string.{EndWith, Match}
import io.github.iltotore.iron.{:|, DescribedAs, Not}

type EmailAddr = String :| Match["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]

type IamArn = String :| Match["^arn:(aws[a-zA-Z-]*)?:iam::\\d{12}:role/[A-Za-z0-9-]+$"]

type SnsArn = String :| Match["^arn:(aws[a-zA-Z-]*)?:sns:[A-Za-z0-9_-]+:\\d{12}:[A-Za-z0-9-]+$"]

type KmsArn = String :| Match["^arn:(aws[a-zA-Z-]*)?:kms:[A-Za-z0-9-]+:\\d{12}:key/[A-Za-z0-9-]+$"]

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Namespace
type CloudWatchNamespace = String :| Match["""^[a-zA-Z0-9_.\-#:]+$"""]

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
  from: EmailAddr,
  to: NonEmptyList[EmailAddr],
  subject: String,
  body: String,
  cc: List[EmailAddr] = List.empty,
  bcc: List[EmailAddr] = List.empty)

// sqs

object SqsUrl {
  type Standard =
    String :| DescribedAs[ValidURL & Not[EndWith[".fifo"]], "Standard Queue must not end with fifo"]
  type Fifo =
    String :| DescribedAs[ValidURL & EndWith[".fifo"], "FIFO queue must end with .fifo"]
}
