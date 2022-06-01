package mtest.common

import com.github.chenharryhua.nanjin.common.aws.{CloudWatchNamespace, IamArn, SnsArn, SqsConfig, SqsUrl}
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn("arn:aws:iam::123456789012:role/ab-c")
  }

  test("sns") {
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz")
  }

  test("cloudwatch namespace") {
    CloudWatchNamespace("_-abc:213.33#")

    shapeless.test.illTyped(""" CloudWatchNamespace("a\b") """)
  }

  test("sqs url") {
    import eu.timepit.refined.auto.*
    val fifo     = SqsUrl.Fifo("https://github.com/abc.fifo")
    val standard = SqsUrl.Standard("https://github.com/abc")

    shapeless.test.illTyped(""" SqsUrl.Fifo("https://github.com/abc") """)
    shapeless.test.illTyped(""" SqsUrl.Standard("https://github.com/abc.fifo") """)

    SqsConfig.Fifo("https://abc.com/xyz.fifo")
    SqsConfig.Standard("https://abc.com")

    SqsConfig(fifo)
      .withMessageGroupId("abc")
      .withVisibilityTimeout(1)
      .withWaitTimeSeconds(2)
      .withMaxNumberOfMessages(3)

    SqsConfig
      .Fifo(fifo)
      .withMessageGroupId("abc")
      .withVisibilityTimeout(1)
      .withWaitTimeSeconds(2)
      .withMaxNumberOfMessages(3)

    SqsConfig.Standard(standard).withVisibilityTimeout(1).withWaitTimeSeconds(2).withMaxNumberOfMessages(3)

    SqsConfig(standard).withVisibilityTimeout(1).withWaitTimeSeconds(2).withMaxNumberOfMessages(3)

  }
}
