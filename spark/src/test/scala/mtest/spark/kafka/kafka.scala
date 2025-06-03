package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.beijingTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.Hadoop
package object kafka {

  val range: DateTimeRange = DateTimeRange(beijingTime)

  val ctx: KafkaContext[IO] = KafkaContext[IO](KafkaSettings.local)
  val sparKafka: SparKafkaContext[IO] = sparkSession.alongWith(ctx)
  val hadoop: Hadoop[IO] = sparkSession.hadoop[IO]

}
