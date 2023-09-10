package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

class KJsonTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, KJson[Json], KJson[Json]] = ctx.jsonTopic("kjson.test")

  val data = List
    .range(0, 10)
    .map(a =>
      NJProducerRecord(topic.topicName, KJson(Json.fromInt(a)), KJson(Json.fromString("test.string"))))

  val root: NJPath = NJPath("./data/test/spark/kafka/kjson")

  test("load - unload") {
    sparKafka
      .topic(topic)
      .prRdd(data)
      .output
      .stream(1)
      .map(_.toProducerRecord)
      .chunks
      .through(topic.produce.pipe)
      .compile
      .drain
      .unsafeRunSync()
    ctx.schemaRegistry.register(topic.topicDef).unsafeRunSync()
    sparKafka.topic(topic).fromKafka.output.circe(root / "circe").run.unsafeRunSync()
    sparKafka.dump(topic.topicName, root / "jackson", NJDateTimeRange(sydneyTime)).unsafeRunSync()
  }
}
