package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.{
  CloudWatch,
  SimpleEmailService,
  SimpleNotificationService,
  SimpleQueueService
}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, Policy}
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import com.github.chenharryhua.nanjin.guard.observers.sns.SlackObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import eu.timepit.refined.auto.*
import software.amazon.awssdk.regions.Region

import scala.concurrent.duration.DurationInt

object observers {
  val slackObserver: SlackObserver[IO] =
    SlackObserver(SimpleNotificationService[IO](_.region(Region.AP_SOUTHEAST_2)))
      .updateTranslator(_.filter(eventFilters.sampling(crontabs.businessHour)))

  def emailObserver: EmailObserver[IO] =
    EmailObserver(SimpleEmailService[IO](_.region(Region.AP_SOUTHEAST_2)))
      .withPolicy(Policy.crontab(_.every12Hours).offset(8.hours)).withZoneId(sydneyTime)
      .withCapacity(200)

  val cloudwatch: CloudWatchObserver[IO] =
    CloudWatchObserver(CloudWatch[IO](_.region(Region.AP_SOUTHEAST_2)))
      .includeHistogram(_.withP50.withP95.withMax)
      .includeDimensions(_.withServiceID.withServiceName)
      .unifyMeasurementUnit(_.withInfoUnit(_.BYTES))

  val sqsObserver: SqsObserver[IO] =
    SqsObserver(SimpleQueueService[IO](_.region(Region.AP_SOUTHEAST_2)))

}
