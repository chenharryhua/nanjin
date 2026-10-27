package mtest.guard

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.PrettyJsonTranslator
import io.circe.Json
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

/** Non-wire safety net for the latent `Labelled` key-collision risk (report item M3 / NV3).
  *
  * `Labelled` derives JSON keys from the unqualified type name, so two distinct types with the same simple
  * name combined into one object would silently collide (`Json.obj` keeps the last entry, dropping a field).
  * The review verified the current payloads do not collide; this test pins that so a future addition that
  * paired two same-simple-name types onto one key would fail here rather than silently drop a field. It is a
  * test-only guard: production source is not modified.
  */
class PrettyJsonKeyCollisionTest extends CatsEffectSuite {

  /** Every object node in the JSON tree whose key list has a duplicate (should be empty). */
  private def duplicateKeyObjects(json: Json): List[List[String]] = {
    val here: List[List[String]] =
      json.asObject.toList.flatMap { obj =>
        val keys = obj.keys.toList
        if (keys.distinct.size != keys.size) List(keys) else Nil
      }
    val nested: List[List[String]] =
      json.asObject.map(_.values.toList).orElse(json.asArray.map(_.toList)).getOrElse(Nil)
        .flatMap(duplicateKeyObjects)
    here ::: nested
  }

  // A short service run that produces every event subtype: Start, Panic (via restart), MetricsSnapshot
  // (via report policy), ReportedEvent (via a logged message), and Stop.
  private def allEventKinds: IO[List[Event]] =
    TaskGuard[IO]("pretty.keys")
      .service("collision")
      .updateConfig(
        _.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1))
          .withReportPolicy(_.crontab(_.secondly).repeat)
          .withReportedEventThreshold(_.Info))
      .eventStream { agent =>
        agent.logger.info("hello") >> IO.sleep(1200.millis) >> IO.raiseError(new Exception("boom"))
      }
      .map(checkJson)
      .compile
      .toList

  test("1.no PrettyJson event object has duplicate keys (guards against Labelled simple-name collision)") {
    allEventKinds.flatMap { events =>
      events.traverse(PrettyJsonTranslator[IO].translate).map { rendered =>
        val jsons: List[Json] = rendered.flatten
        assert(jsons.nonEmpty, "no events were rendered")
        val offenders = jsons.flatMap(duplicateKeyObjects)
        assert(offenders.isEmpty, s"duplicate JSON keys found in rendered events: $offenders")
      }
    }
  }

  test("2.the run actually exercised every event subtype") {
    allEventKinds.map { events =>
      val names = events.map(_.getClass.getSimpleName).toSet
      // at minimum Start, Panic, Stop, ReportedEvent, and a MetricsSnapshot should appear
      assert(names.exists(_.contains("ServiceStart")), names.toString)
      assert(names.exists(_.contains("ServicePanic")), names.toString)
      assert(names.exists(_.contains("ServiceStop")), names.toString)
      assert(names.exists(_.contains("ReportedEvent")), names.toString)
      assert(names.exists(_.contains("MetricsSnapshot")), names.toString)
    }
  }
}
