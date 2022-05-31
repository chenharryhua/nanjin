package mtest.common

import com.github.chenharryhua.nanjin.common.aws.{CloudWatchNamespace, IamArn, SnsArn, SqsFifoUrl, SqsUrl}
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

  test("sqs fifo") {
    SqsFifoUrl("https://github.com/abc.fifo")

    shapeless.test.illTyped(""" SqsFifoUrl("https://github.com/abc") """)
    shapeless.test.illTyped(""" SqsFifoUrl("abc.fifo") """)
  }

  test("cloudwatch namespace") {
    CloudWatchNamespace("_-abc:213.33#")

    shapeless.test.illTyped(""" CloudWatchNamespace("a\b") """)
  }
}
