package com.github.chenharryhua.nanjin.aws

import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn("arn:aws:iam::123456789012:role/abc")
  }
  test("sns") {
    SnsArn("arn:aws:sns::123456789012:role/efg")
  }
  test("sqs") {
    SqsUrl("https://github.com")
    SqsUrl("http://github.com")
    shapeless.test.illTyped("""SqsUrl("abc")""")
  }
}
