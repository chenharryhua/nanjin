package com.github.chenharryhua.nanjin.common

import io.circe.optics.all.*
import io.circe.syntax.given
import io.circe.{Encoder, Json, JsonObject}
import monocle.function.Plated

import java.text.DecimalFormat
import java.time.Duration

/** JSON transformations intended for '''human-facing output''' only — logs and on-screen display.
  *
  * These transforms deliberately rewrite values for readability and safety in a display context, so the
  * result is '''not''' a faithful, machine-readable representation and must never be used as wire format or
  * fed back into a decoder:
  *   - `prettify` rewrites numbers as grouped strings (e.g. `1234567` becomes `"1,234,567"`) and
  *     duration-encoded strings into human phrases (e.g. `"1 minute 5 seconds"`).
  *   - `redact` replaces the value of sensitive fields with a fixed marker so secrets do not leak into logs
  *     or the dashboard.
  */
object json {
  private val redacted: String = "redacted(*****)"

  private val pretty_json: Json => Json = {
    val decimalFormatter = new DecimalFormat("#,###")
    Plated.transform[Json] { js =>
      js.asNumber match {
        case Some(value) =>
          Json.fromString(decimalFormatter.format(value.toDouble))
        case None =>
          js.as[Duration] match {
            case Left(_)      => js
            case Right(value) => Json.fromString(DurationFormatter.defaultFormatter.format(value))
          }
      }
    }
  }

  /** Rewrite a value's JSON for display: numbers become grouped strings and duration-encoded strings become
    * human-readable phrases, at any nesting depth. For logs and screen output only, not for serialization.
    */
  def prettify[A: Encoder](a: A): Json = pretty_json(a.asJson)

  /** Rewrite, at any nesting depth, the '''string''' value of every object field whose key is in `keys`,
    * applying `f` to the matched string, so sensitive fields do not appear in logs or on-screen output.
    *
    * Only string values are rewritten. A matching key whose value is an object, array, or number is left
    * structurally intact — this deliberately preserves shape rather than collapsing a container into a
    * marker, and `Plated.transform` still recurses into that container, so sensitive string leaves nested
    * inside it are caught on their own. Redaction is keyed on the field name, checked at the object level
    * where the key is available. For display only, not for serialization.
    */
  def redact(keys: List[String], f: String => String): Json => Json =
    Plated.transform[Json] { js =>
      js.asObject match {
        case Some(obj) =>
          Json.fromJsonObject(obj.toIterable.foldLeft(JsonObject.empty) { case (acc, (key, value)) =>
            value.asString match {
              case Some(str) if keys.contains(key) =>
                acc.add(key, Json.fromString(f(str)))
              case _ =>
                acc.add(key, value)
            }
          })
        case None => js
      }
    }

  /** Replace matched string values with a fixed marker. Pass a held collection with a splat:
    * `redact(configuredKeys*)`.
    */
  def redact(keys: String*): Json => Json =
    redact(keys.toList, _ => redacted)
}
