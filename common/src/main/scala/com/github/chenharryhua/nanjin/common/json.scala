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
  private val redacted: Json = Json.fromString("redacted(*****)")

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

  /** Replace, at any nesting depth, the value of every object field whose key is in `keys` with a fixed
    * marker, so sensitive fields do not appear in logs or on-screen output.
    *
    * Redaction is keyed on the field name, so the check happens at the object level where the key is
    * available. `Plated.transform` recurses into nested objects and arrays. For display only, not for
    * serialization. Pass an existing collection with a splat: `redact(configuredKeys*)`.
    */
  def redact(keys: String*): Json => Json =
    Plated.transform[Json] { js =>
      js.asObject match {
        case Some(obj) =>
          Json.fromJsonObject(obj.toIterable.foldLeft(JsonObject.empty) { case (acc, (key, value)) =>
            acc.add(key, if (keys.contains(key)) redacted else value)
          })
        case None => js
      }
    }
}
