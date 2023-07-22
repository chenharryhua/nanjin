package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.time.zones.beijingTime
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.*
package object kafka {

  val range: NJDateTimeRange = NJDateTimeRange(beijingTime)

  val ctx: KafkaContext[IO] =
    KafkaSettings.local
      .withApplicationId("spark.kafka.test.app")
      .withGroupId("spark.kafka.test.group")
      .ioContext
  val sparKafka: SparKafkaContext[IO] = sparkSession.alongWith(ctx)
}
