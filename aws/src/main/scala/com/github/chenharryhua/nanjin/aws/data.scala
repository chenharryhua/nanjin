package com.github.chenharryhua.nanjin.aws

import cats.Show
import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Codec, Decoder, Encoder}

opaque type Email = String
object Email:
  def apply(email: String): Email = email
  extension (email: Email) inline def value: String = email

  given Show[Email] = OpaqueLift.lift[Email, String, Show]
  given Encoder[Email] = OpaqueLift.lift[Email, String, Encoder]
  given Decoder[Email] = OpaqueLift.lift[Email, String, Decoder]
end Email

opaque type IamArn = String
object IamArn:
  def apply(iam: String): IamArn = iam
  extension (iam: IamArn) inline def value: String = iam

  given Show[IamArn] = OpaqueLift.lift[IamArn, String, Show]
  given Encoder[IamArn] = OpaqueLift.lift[IamArn, String, Encoder]
  given Decoder[IamArn] = OpaqueLift.lift[IamArn, String, Decoder]
end IamArn

opaque type SnsArn = String
object SnsArn:
  def apply(sns: String): SnsArn = sns
  extension (sns: SnsArn) inline def value: String = sns

  given Show[SnsArn] = OpaqueLift.lift[SnsArn, String, Show]
  given Encoder[SnsArn] = OpaqueLift.lift[SnsArn, String, Encoder]
  given Decoder[SnsArn] = OpaqueLift.lift[SnsArn, String, Decoder]
end SnsArn

opaque type KmsArn = String
object KmsArn:
  def apply(kms: String): KmsArn = kms
  extension (kms: KmsArn) inline def value: String = kms

  given Show[KmsArn] = OpaqueLift.lift[KmsArn, String, Show]
  given Encoder[KmsArn] = OpaqueLift.lift[KmsArn, String, Encoder]
  given Decoder[KmsArn] = OpaqueLift.lift[KmsArn, String, Decoder]
end KmsArn

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
end EmailContent

// sqs

object SqsUrl {
  opaque type Standard = String
  object Standard:
    def apply(std: String): Standard = std
    extension (std: Standard) inline def value: String = std

    given Show[Standard] = OpaqueLift.lift[Standard, String, Show]
    given Encoder[Standard] = OpaqueLift.lift[Standard, String, Encoder]
    given Decoder[Standard] = OpaqueLift.lift[Standard, String, Decoder]
  end Standard

  opaque type Fifo = String
  object Fifo:
    def apply(fifo: String): Fifo = fifo
    extension (fifo: Fifo) inline def value: String = fifo

    given Show[Fifo] = OpaqueLift.lift[Fifo, String, Show]
    given Encoder[Fifo] = OpaqueLift.lift[Fifo, String, Encoder]
    given Decoder[Fifo] = OpaqueLift.lift[Fifo, String, Decoder]
  end Fifo
}
