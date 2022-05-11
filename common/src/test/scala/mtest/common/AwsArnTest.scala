package mtest.common

import com.github.chenharryhua.nanjin.common.aws.{CloudWatchNamespace, IamArn, SnsArn, SqsUrl}
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn("arn:aws:iam::123456789012:role/ab-c")
  }

  test("sns") {
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz")
  }

  test("sqs") {
    SqsUrl("https://github.com")
    SqsUrl("http://github.com")
    shapeless.test.illTyped("""SqsUrl("abc")""")
  }

  test("cloudwatch namespace") {
    CloudWatchNamespace("_-abc:213.33#")

    shapeless.test.illTyped(""" CloudWatchNamespace("a\b") """)
  }
}
