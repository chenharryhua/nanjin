package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import eu.timepit.refined.auto.*
import io.circe.Json
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

class KJsonTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, KJson[Json], KJson[Json]] = ctx.jsonTopic("kjson.test")

  val data: List[NJProducerRecord[KJson[Json], KJson[Json]]] = List
    .range(0, 10)
    .map(a =>
      NJProducerRecord(topic.topicName, KJson(Json.fromInt(a)), KJson(Json.fromString("test.string"))))

  val root = "./data/test/spark/kafka/kjson"

  test("load - unload") {
    sparKafka
      .topic(topic.topicDef)
      .prRdd(data)
      .stream[IO](1)
      .map(_.toProducerRecord)
      .chunks
      .through(topic.produce.sink)
      .compile
      .drain
      .unsafeRunSync()
    ctx.schemaRegistry.register(topic.topicDef).unsafeRunSync()
    sparKafka.topic(topic.topicDef).fromKafka.flatMap(_.output.circe(root / "circe").run[IO]).unsafeRunSync()
    sparKafka.dump(topic.topicName, root / "jackson", DateTimeRange(sydneyTime)).unsafeRunSync()
  }
}
