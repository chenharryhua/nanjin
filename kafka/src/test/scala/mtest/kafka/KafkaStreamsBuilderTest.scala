package mtest.kafka

import cats.effect.IO
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{KafkaStreamSettings, SerdeSettings}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateUpdate}
import fs2.concurrent.Channel
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Consumed
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class KafkaStreamsBuilderTest extends AnyFunSuite with Matchers {
  private class FakeSchemaRegistryClient extends SchemaRegistryClient {
    override def parseSchema(
      schemaType: String,
      schemaString: String,
      references: util.List[SchemaReference]): Optional[ParsedSchema] = Optional.empty()

    override def register(subject: String, schema: ParsedSchema): Int = -1
    override def register(subject: String, schema: ParsedSchema, version: Int, id: Int): Int = -1
    override def getSchemaById(id: Int): ParsedSchema = null
    override def getSchemaBySubjectAndId(subject: String, id: Int): ParsedSchema = null
    override def getAllSubjectsById(id: Int): util.Collection[String] = util.Collections.emptyList()
    override def getLatestSchemaMetadata(subject: String): SchemaMetadata = null
    override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = null
    override def getVersion(subject: String, schema: ParsedSchema): Int = -1
    override def getAllVersions(subject: String): util.List[Integer] = util.Collections.emptyList()
    override def testCompatibility(subject: String, schema: ParsedSchema): Boolean = false
    override def setMode(mode: String): String = ""
    override def setMode(mode: String, subject: String): String = ""
    override def getMode: String = ""
    override def getMode(subject: String): String = ""
    override def getAllSubjects: util.Collection[String] = util.Collections.emptyList()
    override def getId(subject: String, schema: ParsedSchema): Int = -1
    override def reset(): Unit = ()
  }

  private val applicationId = "app-id"
  private val streamSettings = KafkaStreamSettings(Map("foo" -> "bar"))
  private val serdeSettings = SerdeSettings(Map.empty)
  private val srClient = new FakeSchemaRegistryClient

  private val builder = KafkaStreamsBuilder[IO](
    applicationId,
    streamSettings,
    srClient,
    serdeSettings,
    (sb, _) => sb.stream("in", Consumed.`with`(Serdes.String(), Serdes.String())).to("out")
  )

  test("should include application id in properties") {
    builder.properties("application.id") shouldBe applicationId
    builder.properties("foo") shouldBe "bar"
  }

  test("withProperty should produce an updated builder without mutating the original") {
    val updated = builder.withProperty("foo", "baz")

    updated.properties("foo") shouldBe "baz"
    builder.properties("foo") shouldBe "bar"
  }

  test("withProperties should merge multiple properties immutably") {
    val updated = builder.withProperties(Map("k1" -> "v1", "k2" -> "v2"))

    updated.properties("k1") shouldBe "v1"
    updated.properties("k2") shouldBe "v2"
    builder.properties.contains("k1") shouldBe false
    builder.properties.contains("k2") shouldBe false
  }

  test("withStartUpTimeout should return a distinct builder instance") {
    val updated = builder.withStartUpTimeout(FiniteDuration(1234, TimeUnit.MILLISECONDS))

    assert(updated.ne(builder))
  }

  test("topology should evaluate the top closure lazily and build a Kafka topology") {
    var executed = false
    val lazyBuilder = KafkaStreamsBuilder[IO](
      "app2",
      KafkaStreamSettings(Map.empty),
      srClient,
      serdeSettings,
      { (sb, _) =>
        executed = true
        sb.stream("in", Consumed.`with`(Serdes.String(), Serdes.String())).to("out")
      }
    )

    executed shouldBe false
    val topology = lazyBuilder.topology
    executed shouldBe true
    topology should not be null
    topology.describe().toString should include("KSTREAM-SOURCE")
  }

  private def newStateChange(
    dispatcher: Dispatcher[IO],
    latch: CountDownLatch[IO],
    stop: cats.effect.Deferred[IO, Either[Throwable, Unit]],
    bus: Option[Channel[IO, StateUpdate]]): Any = {
    val clazz = builder.getClass.getDeclaredClasses
      .find(_.getSimpleName.contains("StateChange"))
      .getOrElse(sys.error("StateChange class not found"))
    val ctor = clazz.getDeclaredConstructors.head
    ctor.setAccessible(true)
    ctor.newInstance(builder, dispatcher, latch, stop, bus)
  }

  test("StateChange should release a latch and emit a bus update on RUNNING") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        latch <- CountDownLatch[IO](1)
        stop <- IO.deferred[Either[Throwable, Unit]]
        bus <- Channel.unbounded[IO, StateUpdate]
        stateChange = newStateChange(dispatcher, latch, stop, Some(bus))
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.RUNNING, State.CREATED)
        }
        _ <- latch.await
        updates <- bus.stream.take(1).compile.toList
      } yield updates shouldBe List(StateUpdate(State.RUNNING, State.CREATED))
    }.unsafeRunSync()
  }

  test("StateChange should complete stop with error on ERROR") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        latch <- CountDownLatch[IO](1)
        stop <- IO.deferred[Either[Throwable, Unit]]
        stateChange = newStateChange(dispatcher, latch, stop, None)
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.ERROR, State.RUNNING)
        }
        result <- stop.get
      } yield result match {
        case Left(err) => err.getMessage shouldBe "Kafka Streams were stopped abnormally"
        case Right(_)  => fail("expected KafkaStreamsAbnormallyStopped")
      }
    }.unsafeRunSync()
  }
}
