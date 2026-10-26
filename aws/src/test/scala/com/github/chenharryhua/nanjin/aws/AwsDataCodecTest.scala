package com.github.chenharryhua.nanjin.aws

import cats.data.NonEmptyList
import cats.syntax.show.*
import io.circe.syntax.*
import munit.FunSuite

class AwsDataCodecTest extends FunSuite {

  test("1.Email preserves its value, Show, JSON representation, and codec round-trip") {
    val email = Email("user@example.com")
    val json = email.asJson

    assertEquals(email.value, "user@example.com")
    assertEquals(email.show, "user@example.com")
    assertEquals(json.noSpaces, "\"user@example.com\"")
    assertEquals(json.as[Email], Right(email))
  }

  test("2.Arn values preserve their values, Show, JSON representations, and codec round-trips") {
    val iam_arn = IamArn("arn:aws:iam::123456789012:role/test")
    val sns_arn = SnsArn("arn:aws:sns:us-east-1:123456789012:topic")
    val kms_arn = KmsArn("arn:aws:kms:us-east-1:123456789012:key/key-id")

    assertEquals(iam_arn.value, "arn:aws:iam::123456789012:role/test")
    assertEquals(iam_arn.show, iam_arn.value)
    assertEquals(iam_arn.asJson.noSpaces, "\"arn:aws:iam::123456789012:role/test\"")
    assertEquals(iam_arn.asJson.as[IamArn], Right(iam_arn))

    assertEquals(sns_arn.value, "arn:aws:sns:us-east-1:123456789012:topic")
    assertEquals(sns_arn.show, sns_arn.value)
    assertEquals(sns_arn.asJson.noSpaces, "\"arn:aws:sns:us-east-1:123456789012:topic\"")
    assertEquals(sns_arn.asJson.as[SnsArn], Right(sns_arn))

    assertEquals(kms_arn.value, "arn:aws:kms:us-east-1:123456789012:key/key-id")
    assertEquals(kms_arn.show, kms_arn.value)
    assertEquals(kms_arn.asJson.noSpaces, "\"arn:aws:kms:us-east-1:123456789012:key/key-id\"")
    assertEquals(kms_arn.asJson.as[KmsArn], Right(kms_arn))
  }

  test("3.ParameterStoreContent preserves value, Show, JSON representation, and codec round-trip") {
    val content = ParameterStoreContent("stored-content")
    val json = content.asJson

    assertEquals(content.value, "stored-content")
    assertEquals(content.show, "stored-content")
    assertEquals(json.noSpaces, "\"stored-content\"")
    assertEquals(json.as[ParameterStoreContent], Right(content))
  }

  test("4.SqsUrl values preserve values, Show, JSON representations, and codec round-trips") {
    val standard = SqsUrl.Standard("https://sqs.us-east-1.amazonaws.com/123456789012/standard")
    val fifo = SqsUrl.Fifo("https://sqs.us-east-1.amazonaws.com/123456789012/events.fifo")

    assertEquals(standard.value, "https://sqs.us-east-1.amazonaws.com/123456789012/standard")
    assertEquals(standard.show, standard.value)
    assertEquals(standard.asJson.noSpaces, "\"https://sqs.us-east-1.amazonaws.com/123456789012/standard\"")
    assertEquals(standard.asJson.as[SqsUrl.Standard], Right(standard))

    assertEquals(fifo.value, "https://sqs.us-east-1.amazonaws.com/123456789012/events.fifo")
    assertEquals(fifo.show, fifo.value)
    assertEquals(fifo.asJson.noSpaces, "\"https://sqs.us-east-1.amazonaws.com/123456789012/events.fifo\"")
    assertEquals(fifo.asJson.as[SqsUrl.Fifo], Right(fifo))
  }

  test("5.EmailContent keeps defaults and its existing object wire format through a round-trip") {
    val content = EmailContent(
      from = Email("from@example.com"),
      to = NonEmptyList.of(Email("to@example.com"), Email("other@example.com")),
      subject = "subject",
      body = "<p>body</p>"
    )
    val json = content.asJson

    assertEquals(content.cc, Nil)
    assertEquals(content.bcc, Nil)
    assertEquals(
      json.noSpaces,
      """{"from":"from@example.com","to":["to@example.com","other@example.com"],"subject":"subject","body":"<p>body</p>","cc":[],"bcc":[]}"""
    )
    assertEquals(json.as[EmailContent], Right(content))
  }
}
