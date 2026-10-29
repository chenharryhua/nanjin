package com.github.chenharryhua.nanjin.guard.translator

import cats.Monad
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ReportedEvent,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.EventPipe
import mtest.guard.gendata.{
  allEvents,
  metricsSnapshot,
  reportedEvent,
  servicePanic,
  serviceStart,
  serviceStop
}
import munit.CatsEffectSuite

/** Deterministic coverage for every `Translator` handler overload and combinator branch.
  *
  * The events come from the existing lightweight `gendata` fixture: handlers in this suite inspect only the
  * runtime subtype, so no service lifecycle or wall-clock delay is required.
  */
class TranslatorOverloadsTest extends CatsEffectSuite {

  private val EXPECTED = List(Some(1), Some(2), Some(3), Some(4), Some(5))

  private def translated(translator: Translator[IO, Int]): IO[List[Option[Int]]] =
    translator.traverse(allEvents)

  private val tagged: Translator[IO, Int] =
    Translator
      .empty[IO, Int]
      .withServiceStart((_: ServiceStart) => 1)
      .withServicePanic((_: ServicePanic) => 2)
      .withServiceStop((_: ServiceStop) => 3)
      .withMetricsSnapshot((_: MetricsSnapshot) => 4)
      .withReportedEvent((_: ReportedEvent) => 5)

  test("1.effectful optional handlers cover every event subtype") {
    val translator = Translator
      .empty[IO, Int]
      .withServiceStart((_: ServiceStart) => IO.pure(Some(1)))
      .withServicePanic((_: ServicePanic) => IO.pure(Some(2)))
      .withServiceStop((_: ServiceStop) => IO.pure(Some(3)))
      .withMetricsSnapshot((_: MetricsSnapshot) => IO.pure(Some(4)))
      .withReportedEvent((_: ReportedEvent) => IO.pure(Some(5)))

    translated(translator).map(result => assertEquals(result, EXPECTED))
  }

  test("2.pure optional handlers cover every event subtype") {
    val translator = Translator
      .empty[IO, Int]
      .withServiceStart((_: ServiceStart) => Some(1))
      .withServicePanic((_: ServicePanic) => Some(2))
      .withServiceStop((_: ServiceStop) => Some(3))
      .withMetricsSnapshot((_: MetricsSnapshot) => Some(4))
      .withReportedEvent((_: ReportedEvent) => Some(5))

    translated(translator).map(result => assertEquals(result, EXPECTED))
  }

  test("3.effectful total handlers cover every event subtype") {
    val translator = Translator
      .empty[IO, Int]
      .withServiceStart((_: ServiceStart) => IO.pure(1))
      .withServicePanic((_: ServicePanic) => IO.pure(2))
      .withServiceStop((_: ServiceStop) => IO.pure(3))
      .withMetricsSnapshot((_: MetricsSnapshot) => IO.pure(4))
      .withReportedEvent((_: ReportedEvent) => IO.pure(5))

    translated(translator).map(result => assertEquals(result, EXPECTED))
  }

  test("4.pure total handlers cover every event subtype") {
    translated(tagged).map(result => assertEquals(result, EXPECTED))
  }

  test("5.every individual skip method drops only its event subtype") {
    for {
      start <- tagged.skipServiceStart.translate(serviceStart)
      panic <- tagged.skipServicePanic.translate(servicePanic)
      stop <- tagged.skipServiceStop.translate(serviceStop)
      metrics <- tagged.skipMetricsSnapshot.translate(metricsSnapshot)
      reported <- tagged.skipReportedEvent.translate(reportedEvent)
      untouched <- tagged.skipServiceStart.translate(reportedEvent)
    } yield {
      assertEquals(List(start, panic, stop, metrics, reported), List.fill(5)(None))
      assertEquals(untouched, Some(5))
    }
  }

  test("6.predicate and EventPipe filters execute every handler branch") {
    val keep_all = tagged.filter((_: Event) => true)
    val drop_all = tagged.filter((_: Event) => false)
    val only_reported =
      tagged.filter(EventPipe(event => Option.when(event.isInstanceOf[ReportedEvent])(event)))

    for {
      kept <- translated(keep_all)
      dropped <- translated(drop_all)
      piped <- translated(only_reported)
    } yield {
      assertEquals(kept, EXPECTED)
      assertEquals(dropped, List.fill(5)(None))
      assertEquals(piped, List(None, None, None, None, Some(5)))
    }
  }

  test("7.flatMap executes the matching continuation and preserves skipped results") {
    val mapped = tagged.flatMap(value =>
      Translator
        .empty[IO, Int]
        .withServiceStart((_: ServiceStart) => value * 10)
        .withServicePanic((_: ServicePanic) => value * 10)
        .withServiceStop((_: ServiceStop) => value * 10)
        .withMetricsSnapshot((_: MetricsSnapshot) => value * 10)
        .withReportedEvent((_: ReportedEvent) => value * 10))

    for {
      values <- translated(mapped)
      skipped <- translated(Translator.empty[IO, Int].flatMap(_ => tagged))
    } yield {
      assertEquals(values, List(Some(10), Some(20), Some(30), Some(40), Some(50)))
      assertEquals(skipped, List.fill(5)(None))
    }
  }

  test("8.tailRecM handles recursion, completion, and skipped translations for every subtype") {
    type TranslatorIO[A] = Translator[IO, A]
    val monad = summon[Monad[TranslatorIO]]
    val recursive = monad.tailRecM[Int, String](0) { value =>
      monad.pure(if value < 2 then Left(value + 1) else Right("done"))
    }
    val skipped = monad.tailRecM[Int, String](0)(_ => Translator.empty[IO, Either[Int, String]])

    for {
      completed <- recursive.traverse(allEvents)
      absent <- skipped.traverse(allEvents)
    } yield {
      assertEquals(completed, List.fill(5)(Some("done")))
      assertEquals(absent, List.fill(5)(None))
    }
  }

  test("9.skipAll drops every subtype") {
    translated(tagged.skipAll).map(result => assertEquals(result, List.fill(5)(None)))
  }
}
