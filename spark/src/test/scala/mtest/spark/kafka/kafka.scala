package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.beijingTime
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.NJHadoop
package object kafka {

  val range: NJDateTimeRange = NJDateTimeRange(beijingTime)

  val ctx: KafkaContext[IO]           = KafkaContext[IO](KafkaSettings.local)
  val sparKafka: SparKafkaContext[IO] = sparkSession.alongWith(ctx)
  val hadoop: NJHadoop[IO]            = sparkSession.hadoop[IO]

}
