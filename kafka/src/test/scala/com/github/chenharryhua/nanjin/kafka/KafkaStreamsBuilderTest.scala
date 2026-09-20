package com.github.chenharryhua.nanjin.kafka

import cats.effect.{IO, Ref}
import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
import com.github.chenharryhua.nanjin.kafka.config.{KafkaStreamSettings, SerdeSettings}
import com.github.chenharryhua.nanjin.kafka.streaming.{
  KafkaStreamsAbnormallyStopped,
  KafkaStreamsBuilder,
  StateTransition
}
import fs2.Stream
import io.circe.Encoder
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Consumed
import munit.CatsEffectSuite

import java.util
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

class KafkaStreamsBuilderTest extends CatsEffectSuite {
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
  private val streamSettings = KafkaStreamSettings(Map("state.dir" -> "bar"))
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
    assertEquals(builder.properties("application.id"), applicationId)
    assertEquals(builder.properties("state.dir"), "bar")
  }

  test("2.withProperty should produce an updated builder without mutating the original") {
    val updated = builder.withProperty(_.STATE_DIR_CONFIG, "baz")

    assertEquals(updated.properties("state.dir"), "baz")
    assertEquals(builder.properties("state.dir"), "bar")
  }

  test("3.withProperties should merge multiple properties immutably") {
    val updated = builder.withProperties(Map("k1" -> "v1", "k2" -> "v2"))

    assertEquals(updated.properties("k1"), "v1")
    assertEquals(updated.properties("k2"), "v2")
    assertEquals(builder.properties.contains("k1"), false)
    assertEquals(builder.properties.contains("k2"), false)
  }

  test("4.withStartupTimeout should return a distinct builder instance") {
    val updated = builder.withStartupTimeout(FiniteDuration(1234, TimeUnit.MILLISECONDS))

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

    assertEquals(executed, false)
    val topology = lazyBuilder.topology
    assertEquals(executed, true)
    assert(topology != null)
    assert(topology.describe().toString.contains("KSTREAM-SOURCE"))
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
        logged <- Ref.of[IO, List[(LogLevel, StateTransition)]](Nil)
        recordingLog = new Log[IO] {
          override protected type M = (LogLevel, StateTransition)
          override protected def create[S: Encoder](msg: S, level: LogLevel, ex: Option[Throwable]): IO[M] =
            IO.pure((level, msg.asInstanceOf[StateTransition]))
          override protected def publish(event: M): IO[Unit] = logged.update(_ :+ event)
          override protected def enabled(level: LogLevel): IO[Boolean] = IO.pure(true)
        }
        testBuilder = builder.withTransitionLog(recordingLog)
        stateChange = newStateChange(testBuilder, dispatcher, startup, stop)
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.RUNNING, State.CREATED)
        }
        _ <- startup.get
        entries <- logged.get
      } yield {
        assertEquals(entries.map(_._1), List(LogLevel.Good))
        assertEquals(entries.map(_._2), List(StateTransition(applicationId, State.CREATED, State.RUNNING)))
      }
    }
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
        case Left(err) => assertEquals(err.getMessage, "KafkaStreams(app-id) were stopped abnormally")
        case Right(_)  => fail("expected KafkaStreamsAbnormallyStopped")
      }
    }
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
      } yield assertEquals(result, Right(()))
    }
  }

  test("9.withLog should log the correct level per state") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        logged <- Ref.of[IO, List[LogLevel]](Nil)
        recordingLog = new Log[IO] {
          override protected type M = LogLevel
          override protected def create[S: Encoder](msg: S, level: LogLevel, ex: Option[Throwable]): IO[M] =
            IO.pure(level)
          override protected def publish(event: M): IO[Unit] = logged.update(_ :+ event)
          override protected def enabled(level: LogLevel): IO[Boolean] = IO.pure(true)
        }
        testBuilder = builder.withTransitionLog(recordingLog)
        stateChange = newStateChange(testBuilder, dispatcher, startup, stop)
        _ <- IO {
          val m = stateChange.getClass.getMethod("onChange", classOf[State], classOf[State])
          m.invoke(stateChange, State.RUNNING, State.CREATED)
          m.invoke(stateChange, State.NOT_RUNNING, State.RUNNING)
        }
        levels <- logged.get
      } yield assertEquals(levels, List(LogLevel.Good, LogLevel.Info))
    }
  }

  test("10.kafkaStreams should fail when startup does not complete before the timeout") {
    val failingBuilder = builder
      .withProperty(_.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      .withStartupTimeout(FiniteDuration(1, TimeUnit.MILLISECONDS))

    failingBuilder.kafkaStreams.compile.drain.attempt.map { result =>
      assertEquals(result.isLeft, true)
    }
  }

  test("11.kafkaStreams should fail when the topology builder throws") {
    val throwingBuilder = KafkaStreamsBuilder[IO](
      applicationId,
      streamSettings.withProperty("bootstrap.servers", "localhost:9092"),
      srClient,
      serdeSettings,
      (_, _) => throw new IllegalStateException("topology failed")
    )

    throwingBuilder.kafkaStreams.compile.drain.attempt.map { result =>
      assertEquals(result.isLeft, true)
    }
  }

  test("12.StateChange should complete stop on close path (PENDING_SHUTDOWN -> NOT_RUNNING)") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        stateChange = newStateChange(builder, dispatcher, startup, stop)
        _ <- IO {
          val m = stateChange.getClass.getMethod("onChange", classOf[State], classOf[State])
          m.invoke(stateChange, State.PENDING_SHUTDOWN, State.RUNNING)
          m.invoke(stateChange, State.NOT_RUNNING, State.PENDING_SHUTDOWN)
        }
        result <- stop.get
      } yield assertEquals(result, Right(()))
    }
  }

  test("13.fs2 stream should terminate when stop is completed with Right") {
    val result = for {
      stop <- IO.deferred[Either[Throwable, Unit]]
      fiber <- Stream.never[IO].interruptWhen(stop).compile.drain.start
      _ <- stop.complete(Right(())).void
      done <- fiber.joinWithNever.timeout(1.second).attempt
    } yield done

    result.map(done => assertEquals(done, Right(())))
  }

  test("14.fs2 stream should fail when internal KafkaStreams error is signaled") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        startup <- IO.deferred[Unit]
        stop <- IO.deferred[Either[Throwable, Unit]]
        stateChange = newStateChange(builder, dispatcher, startup, stop)
        fiber <- Stream.never[IO].interruptWhen(stop).compile.drain.attempt.start
        _ <- IO {
          stateChange.getClass
            .getMethod("onChange", classOf[State], classOf[State])
            .invoke(stateChange, State.ERROR, State.RUNNING)
        }
        result <- fiber.joinWithNever.timeout(1.second)
      } yield result match {
        case Left(err: KafkaStreamsAbnormallyStopped) => assertEquals(err.applicationId, applicationId)
        case Left(other)                              => fail(s"unexpected error: ${other.getClass.getName}")
        case Right(_)                                 => fail("expected fs2 stream failure on internal error")
      }
    }
  }

  test("15.StateChange randomized terminal invariant: first terminal signal wins") {
    Dispatcher.sequential[IO].use { dispatcher =>
      def firstTerminal(states: List[State]): Option[Either[Throwable, Unit]] =
        states.collectFirst {
          case State.ERROR       => Left(new RuntimeException("error"))
          case State.NOT_RUNNING => Right(())
        }

      val nonTerminalStates = List(State.PENDING_ERROR, State.PENDING_SHUTDOWN, State.REBALANCING)
      val terminalStates = List(State.ERROR, State.NOT_RUNNING)
      val rng = new Random(20260806L)

      val scenarios: List[List[State]] = List.fill(80) {
        val prefix = List.fill(rng.between(0, 5))(nonTerminalStates(rng.nextInt(nonTerminalStates.size)))
        val terminal = terminalStates(rng.nextInt(terminalStates.size))
        val suffix = List.fill(rng.between(0, 3))(nonTerminalStates(rng.nextInt(nonTerminalStates.size)))
        prefix ++ (terminal :: suffix)
      }

      scenarios.foldLeft(IO.unit) { (acc, seq) =>
        acc >> {
          for {
            startup <- IO.deferred[Unit]
            stop <- IO.deferred[Either[Throwable, Unit]]
            stateChange = newStateChange(builder, dispatcher, startup, stop)
            _ <- IO {
              val m = stateChange.getClass.getMethod("onChange", classOf[State], classOf[State])
              var old: State = State.CREATED
              seq.foreach { next =>
                m.invoke(stateChange, next, old)
                old = next
              }
            }
            _ <- startup.get.timeout(1.second)
            stopResult <- stop.get.timeout(1.second)
          } yield firstTerminal(seq) match {
            case Some(Left(_)) =>
              assertEquals(stopResult.isLeft, true)
              ()
            case Some(Right(_)) =>
              assertEquals(stopResult, Right(()))
              ()
            case None => fail("scenario generation error: terminal state missing")
          }
        }
      }
    }
  }
}
