package com.github.chenharryhua.nanjin.aws

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("1.iam") {
    val iam: IamArn = IamArn("arn:aws:iam::123456789012:role/ab-c")
    println(iam.asJson)
    println(iam.show)
  }

  test("2.sns") {
    val sns: SnsArn = SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz")
    println(sns.asJson)
    println(sns.show)
  }

  test("3.kms") {
    val kms: KmsArn = KmsArn("arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb")
    println(kms.asJson)
    println(kms.show)

  }

  test("4.sqs url") {
    val fifo = SqsUrl.Fifo("https://github.com/abc.fifo")
    val std = SqsUrl.Standard("https://github.com/abc")
    println(fifo.asJson)
    println(std.show)
  }

  test("5.address") {
    val address = Email("who@gmail.com")
    println(address.asJson)
    println(address.show)
  }

  test("6.chunk.size") {
    val ck: ChunkSize = ChunkSize(100)
    println(ck.asJson)
    println(ck.show)
  }

  test("7.email content") {
    summon[Encoder[EmailContent]]
    summon[Decoder[EmailContent]]
  }
}
