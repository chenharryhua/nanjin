package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorFilterOps}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import eu.timepit.refined.auto.*
import io.circe.Json
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite

class KJsonTest extends AnyFunSuite {
  val topicDef: AvroTopic[KJson[Json], KJson[Json]] =
    AvroTopic[KJson[Json], KJson[Json]](TopicName("kjson.text"))
  val topic = topicDef

  val data: List[NJProducerRecord[KJson[Json], KJson[Json]]] = List
    .range(0, 10)
    .map(a =>
      NJProducerRecord(topic.topicName, KJson(Json.fromInt(a)), KJson(Json.fromString("test.string"))))

  val root = "./data/test/spark/kafka/kjson"

  test("load - unload") {
    sparKafka
      .topic(topic)
      .prRdd(data)
      .stream[IO](1)
      .mapFilter(r => (r.key, r.value).mapN(_ -> _))
      .chunks
      .through(ctx.produce[KJson[Json], KJson[Json]](topicDef).sink)
      .compile
      .drain
      .unsafeRunSync()
    ctx.schemaRegistry.register(topic).unsafeRunSync()
    sparKafka.topic(topic).fromKafka.flatMap(_.output.circe(root / "circe").run[IO]).unsafeRunSync()
    sparKafka.dump(topic.topicName.name, root / "jackson").unsafeRunSync()
    sparKafka.dumpCirce(topic, root / "circe2").unsafeRunSync()
  }
}
