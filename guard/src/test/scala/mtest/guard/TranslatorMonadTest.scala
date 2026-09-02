package mtest.guard

import cats.Eq
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.laws.discipline.eq.*
import cats.laws.discipline.{ExhaustiveCheck, FunctorFilterTests, MonadTests}
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick, Tick}
import com.github.chenharryhua.nanjin.guard.config.{Brief, Domain, ServiceIdentity, StackTrace, Timestamp}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.translator.Translator
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import munit.{DisciplineSuite, FunSuite}
import org.scalacheck.{Arbitrary, Gen}

object gendata {
  val tick: Tick = PolicyTick.seed[IO](sydneyTime, Policy.empty).unsafeRunSync().tick
  private val ts: Timestamp = Timestamp(tick.zoned(_.conclude))

  // The translator functions under test are all `_ => constant`, so they never inspect an event's payload.
  // That lets us build one instance of every event subtype with null-filled heavy fields; only the runtime
  // subtype matters, since `translate` dispatches on it. Covering all five gives the law checks a genuinely
  // exhaustive event space (the previous version only exercised the ServiceStart branch).
  val serviceStart: ServiceStart =
    ServiceStart(
      null.asInstanceOf[ServiceIdentity],
      null.asInstanceOf[Policy],
      null.asInstanceOf[Brief],
      tick)
  val servicePanic: ServicePanic =
    ServicePanic(
      null.asInstanceOf[ServiceIdentity],
      null.asInstanceOf[Policy],
      null.asInstanceOf[Brief],
      tick,
      null.asInstanceOf[StackTrace])
  val serviceStop: ServiceStop =
    ServiceStop(
      null.asInstanceOf[ServiceIdentity],
      null.asInstanceOf[Policy],
      null.asInstanceOf[Brief],
      ts,
      StopReason.Successfully)
  val metricsSnapshot: MetricsSnapshot =
    MetricsSnapshot(
      null.asInstanceOf[ServiceIdentity],
      null.asInstanceOf[Policy],
      MetricsSnapshot.Index.Adhoc(ts),
      null.asInstanceOf[com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot],
      null.asInstanceOf[Took]
    )
  val reportedEvent: ReportedEvent =
    ReportedEvent(
      null.asInstanceOf[ServiceIdentity],
      ts,
      null.asInstanceOf[Domain],
      null.asInstanceOf[Correlation],
      LogLevel.Info,
      None,
      null.asInstanceOf[Message])

  val allEvents: List[Event] =
    List(serviceStart, servicePanic, serviceStop, metricsSnapshot, reportedEvent)

  implicit val exhaustiveCheck: ExhaustiveCheck[Event] =
    ExhaustiveCheck.instance(allEvents)

  implicit def translatorEq: Eq[Translator[Option, Int]] =
    Eq.by[Translator[Option, Int], Event => Option[Option[Int]]](_.translate)

  implicit val arbiTranslator: Arbitrary[Translator[Option, Int]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Int]
          .withServiceStart(_ => 1)
          .withServiceStop(_ => 2)
          .withServicePanic(_ => 3)
          .withMetricsSnapshot(_ => 4)
          .withReportedEvent(_ => 6)
      ))

  val add: Int => Int = _ + 1

  implicit val arbiAtoB: Arbitrary[Translator[Option, Int => Int]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Int => Int]
          .withServiceStart(_ => add)
          .withServiceStop(_ => add)
          .withServicePanic(_ => add)
          .withMetricsSnapshot(_ => add)
          .withReportedEvent(_ => add)
      ))

  // A real Eq for the 3-arity law (the previous `_ => true` made associativity/composition a no-op).
  implicit val eqAbc: Eq[Translator[Option, (Int, Int, Int)]] =
    Eq.by[Translator[Option, (Int, Int, Int)], Event => Option[Option[(Int, Int, Int)]]](_.translate)

  implicit val arbFOA: Arbitrary[Translator[Option, Option[Int]]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Option[Int]]
          .withServiceStart(_ => Option(1))
          .withServiceStop(_ => Option(2))
          .withServicePanic(_ => Option(3))
          .withMetricsSnapshot(_ => Option(4))
          .withReportedEvent(_ => Option(6))
      ))
}

class TranslatorMonadTest extends DisciplineSuite {
  import gendata.*
  checkAll("Translator.MonadLaws", MonadTests[Translator[Option, *]].monad[Int, Int, Int])
  checkAll("Translator.FunctorFilter", FunctorFilterTests[Translator[Option, *]].functorFilter[Int, Int, Int])
}

/** Behavioral coverage of the Translator combinators, complementing the law checks above. */
class TranslatorBehaviorTest extends FunSuite {
  import gendata.*

  // `translate` returns F[Option[A]] = Option[Option[Int]]; a defined-but-dropped event is Some(None).
  // Typed here so munit's assertEquals has matching sides (Some(None) alone infers as Some[None.type]).
  private val dropped: Option[Option[Int]] = Some(None)
  private def found(i: Int): Option[Option[Int]] = Some(Some(i))

  // maps each event subtype to a distinct value, so we can tell which branch handled it
  private val tagged: Translator[Option, Int] =
    Translator
      .empty[Option, Int]
      .withServiceStart(_ => 1)
      .withServicePanic(_ => 2)
      .withServiceStop(_ => 3)
      .withMetricsSnapshot(_ => 4)
      .withReportedEvent(_ => 5)

  // Plain `==` avoids munit's Compare typeclass, which is finicky about the nested Option[Option[_]] types.
  test("1.translate dispatches on the event subtype") {
    assert(tagged.translate(serviceStart) == found(1))
    assert(tagged.translate(servicePanic) == found(2))
    assert(tagged.translate(serviceStop) == found(3))
    assert(tagged.translate(metricsSnapshot) == found(4))
    assert(tagged.translate(reportedEvent) == found(5))
  }

  test("2.empty translates every event to None") {
    val e = Translator.empty[Option, Int]
    assert(allEvents.forall(evt => e.translate(evt) == dropped))
  }

  test("3.idTranslator returns the event unchanged for every subtype") {
    val id = Translator.idTranslator[Option]
    assert(allEvents.forall(evt => id.translate(evt) == Some(Some(evt))))
  }

  test("4.map transforms the produced value across all branches") {
    val mapped = tagged.map(_ * 10)
    assert(mapped.translate(serviceStart) == found(10))
    assert(mapped.translate(reportedEvent) == found(50))
  }

  test("5.skip* replaces a single branch with None, leaving others intact") {
    val skipped = tagged.skipServicePanic
    assert(skipped.translate(servicePanic) == dropped)
    assert(skipped.translate(serviceStart) == found(1))
    assert(skipped.translate(reportedEvent) == found(5))
  }

  test("6.skipAll drops every branch to None") {
    val skipped = tagged.skipAll
    assert(allEvents.forall(evt => skipped.translate(evt) == dropped))
  }

  test("7.filter by predicate keeps matching events and drops the rest") {
    val onlyStart = tagged.filter {
      case _: ServiceStart => true
      case _               => false
    }
    assert(onlyStart.translate(serviceStart) == found(1))
    assert(onlyStart.translate(servicePanic) == dropped)
    assert(onlyStart.translate(reportedEvent) == dropped)
  }

  test("8.mapFilter keeps Some results and drops None") {
    val evenOnly = tagged.mapFilter(i => Option.when(i % 2 == 0)(i))
    // tagged: start->1, panic->2, stop->3, metrics->4, reported->5.
    // odd values (start=1, stop=3, reported=5) are dropped; even (panic=2, metrics=4) are kept.
    assert(evenOnly.translate(serviceStart) == dropped)
    assert(evenOnly.translate(servicePanic) == found(2))
    assert(evenOnly.translate(serviceStop) == dropped)
    assert(evenOnly.translate(metricsSnapshot) == found(4))
    assert(evenOnly.translate(reportedEvent) == dropped)
  }

  test("9.flatMap threads the value into the next translator per event") {
    val fm = tagged.flatMap(i => Translator.empty[Option, Int].withServiceStart(_ => i * 100))
    // only ServiceStart is defined in the continuation
    assert(fm.translate(serviceStart) == found(100))
    // other events: outer produces a value but the continuation's branch is empty -> None
    assert(fm.translate(reportedEvent) == dropped)
  }
}
