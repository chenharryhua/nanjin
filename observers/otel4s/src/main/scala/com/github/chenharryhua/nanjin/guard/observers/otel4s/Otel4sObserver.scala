package com.github.chenharryhua.nanjin.guard.observers.otel4s

import cats.Endo
import cats.Eval
import cats.effect.kernel.Concurrent
import cats.syntax.flatMap.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  PrettyJsonTranslator,
  Translator,
  UpdateTranslator
}
import fs2.{Pipe, Stream}
import io.circe.Json
import org.typelevel.otel4s.AnyValue
import org.typelevel.otel4s.logs.{LoggerProvider, Severity}

/** Observes service events and emits them as OpenTelemetry log records via otel4s.
  *
  * Each `Event` is translated to a JSON body and emitted through the supplied `LoggerProvider`, with the
  * event's log level mapped to an OpenTelemetry severity. Events pass through unchanged so other observers
  * can consume the same stream.
  *
  * Usage:
  * {{{
  *   OtelJava.autoConfigured[IO]().use { otel4s =>
  *     val observer = Otel4sObserver[IO](otel4s.loggerProvider)
  *     eventStream.through(observer.observe("my-service")).compile.drain
  *   }
  * }}}
  */
object Otel4sObserver {
  def apply[F[_]: Concurrent, Ctx](provider: LoggerProvider[F, Ctx]): Otel4sObserver[F, Ctx] =
    new Otel4sObserver[F, Ctx](provider, PrettyJsonTranslator[F])
}

final class Otel4sObserver[F[_], Ctx](provider: LoggerProvider[F, Ctx], translator: Translator[F, Json])(using
  F: Concurrent[F])
    extends UpdateTranslator[F, Json, Otel4sObserver[F, Ctx]] {

  override def withTranslator(f: Endo[Translator[F, Json]]): Otel4sObserver[F, Ctx] =
    new Otel4sObserver[F, Ctx](provider, f(translator))

  private def severity_of(event: Event): Severity =
    eventLogLevel[Eval, Severity](event).run {
      case LogLevel.Debug => Eval.now(Severity.debug)
      case LogLevel.Info  => Eval.now(Severity.info)
      case LogLevel.Good  => Eval.now(Severity.info)
      case LogLevel.Warn  => Eval.now(Severity.warn)
      case LogLevel.Error => Eval.now(Severity.error)
    }.value

  /** Emit events to OpenTelemetry under the given instrumentation scope name.
    *
    * @param scopeName
    *   the instrumentation scope (logger) name, typically the service or library name
    */
  def observe(scopeName: String): Pipe[F, Event, Event] = (es: Stream[F, Event]) =>
    Stream.eval(provider.logger(scopeName).get).flatMap { logger =>
      es.evalTap { event =>
        translator.translate(event).flatMap {
          _.traverse { json =>
            val severity: Severity = severity_of(event)
            logger.logRecordBuilder
              .withSeverity(severity)
              .withSeverityText(severity.toString)
              .withTimestamp(event.timestamp.value.toInstant)
              .withBody(AnyValue.string(json.noSpaces))
              .emit
          }
        }
      }
    }
}
