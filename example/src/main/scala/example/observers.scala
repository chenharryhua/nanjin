package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.{
  CloudWatch,
  SimpleEmailService,
  SimpleNotificationService,
  SimpleQueueService
}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.event.EventPipe
import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import com.github.chenharryhua.nanjin.guard.observers.sns.SlackObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import software.amazon.awssdk.regions.Region
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.common.chrono.crontabs

/** Example: constructing the AWS-backed guard observers that consume a service's event stream. Each sends
  * events to a different destination; attach them to a service's stream (typically merged) to publish
  * notifications and metrics.
  *
  *   - `slackObserver` — posts to Slack via SNS, filtered to business hours.
  *   - `emailObserver` — batches events (up to 200) and emails them every 12 hours via SES.
  *   - `cloudwatch` — publishes metrics to CloudWatch.
  *   - `sqsObserver` — forwards events to an SQS queue.
  *
  * All target `ap-southeast-2`; substitute your region and destinations.
  */
object observers {
  val slackObserver: SlackObserver[IO] =
    SlackObserver(SimpleNotificationService[IO](_.region(Region.AP_SOUTHEAST_2)))
      .withTranslator(_.filter(EventPipe.cronFilter(crontabs.businessHour)))

  def emailObserver: EmailObserver[IO] =
    EmailObserver(
      EmailObserver
        .Params(SimpleEmailService[IO](_.region(Region.AP_SOUTHEAST_2)))
        .withPolicy(_.crontab(_.every12Hours).offset(8.hours)) // send a digest every 12h, offset by 8h
        .withZoneId(sydneyTime)
        .withCapacity(200)) // buffer up to 200 events per digest

  val cloudwatch: CloudWatchObserver[IO] =
    CloudWatchObserver(CloudWatch[IO](_.region(Region.AP_SOUTHEAST_2)))

  val sqsObserver: SqsObserver[IO] =
    SqsObserver(SimpleQueueService[IO](sydneyTime, _.fixedDelay(10.seconds))(_.region(Region.AP_SOUTHEAST_2)))

}
