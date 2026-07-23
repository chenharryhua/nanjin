package mtest.kafka

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{KafkaStreamSettings, SerdeSettings}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateTransition}
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
import scala.concurrent.duration.{DurationInt, FiniteDuration}

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

  test("1.should include application id in properties") {
    builder.properties("application.id") shouldBe applicationId
    builder.properties("foo") shouldBe "bar"
  }

  test("2.withProperty should produce an updated builder without mutating the original") {
    val updated = builder.withProperty("foo", "baz")

    updated.properties("foo") shouldBe "baz"
    builder.properties("foo") shouldBe "bar"
  }

  test("3.withProperties should merge multiple properties immutably") {
    val updated = builder.withProperties(Map("k1" -> "v1", "k2" -> "v2"))

    updated.properties("k1") shouldBe "v1"
    updated.properties("k2") shouldBe "v2"
    builder.properties.contains("k1") shouldBe false
    builder.properties.contains("k2") shouldBe false
  }

  test("4.withStartUpTimeout should return a distinct builder instance") {
    val updated = builder.withStartUpTimeout(FiniteDuration(1234, TimeUnit.MILLISECONDS))

    assert(updated.ne(builder))
  }

  test("5.topology should evaluate the top closure lazily and build a Kafka topology") {
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
    testBuilder: KafkaStreamsBuilder[IO],
    dispatcher: Dispatcher[IO],
    startup: Deferred[IO, Unit],
    stop: Deferred[IO, Either[Throwable, Unit]]): Any = {
    val clazz = testBuilder.getClass.getDeclaredClasses
      .find(_.getSimpleName.contains("StateTransition"))
      .getOrElse(sys.error("StateTransition class not found"))
    val ctor = clazz.getDeclaredConstructors.head
    ctor.setAccessible(true)
    ctor.newInstance(testBuilder, dispatcher, startup, stop)
  }

  test("6.StateChange should release a latch and invoke the transition callback on RUNNING") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        transition <- IO.deferred[StateTransition]
        testBuilder = builder.onStateTransition(st => transition.complete(st) >> IO.unit)
        stateChange = newStateChange(testBuilder, dispatcher, startup, stop)
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.RUNNING, State.CREATED)
        }
        _ <- startup.get
        received <- transition.get
      } yield received shouldBe StateTransition(applicationId, State.CREATED, State.RUNNING)
    }.unsafeRunSync()
  }

  test("7.StateChange should complete stop with error on ERROR") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        stateChange = newStateChange(builder, dispatcher, startup, stop)
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

  test("8.StateChange should complete stop without error on NOT_RUNNING") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        stateChange = newStateChange(builder, dispatcher, startup, stop)
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.NOT_RUNNING, State.RUNNING)
        }
        result <- stop.get
      } yield result shouldBe Right(())
    }.unsafeRunSync()
  }

  test("9.onStateTransition should invoke the configured callback") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        transition <- IO.deferred[StateTransition]
        testBuilder = builder.onStateTransition(st => transition.complete(st) >> IO.unit)
        stateChange = newStateChange(testBuilder, dispatcher, startup, stop)
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.RUNNING, State.CREATED)
        }
        received <- transition.get.timeoutTo(
          1.second,
          IO.raiseError(new AssertionError("transition callback was not invoked")))
      } yield received.newState shouldBe State.RUNNING
    }.unsafeRunSync()
  }

  test("10.kafkaStreams should fail when startup does not complete before the timeout") {
    val failingBuilder = builder
      .withProperty("bootstrap.servers", "localhost:9092")
      .withStartUpTimeout(FiniteDuration(1, TimeUnit.MILLISECONDS))

    val result = failingBuilder.kafkaStreams.compile.drain.attempt.unsafeRunSync()

    result.isLeft shouldBe true
  }

  test("11.kafkaStreams should fail when the topology builder throws") {
    val throwingBuilder = KafkaStreamsBuilder[IO](
      applicationId,
      streamSettings.withProperty("bootstrap.servers", "localhost:9092"),
      srClient,
      serdeSettings,
      (_, _) => throw new IllegalStateException("topology failed")
    )

    val result = throwingBuilder.kafkaStreams.compile.drain.attempt.unsafeRunSync()

    result.isLeft shouldBe true
  }
}
