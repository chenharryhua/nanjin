package com.github.chenharryhua.nanjin.guard.observers.otel4s

import io.circe.{Json, JsonNumber, JsonObject}
import org.typelevel.otel4s.AnyValue

/** Converts a circe `Json` tree into an otel4s `AnyValue`.
  *
  *   - `Json.Null` becomes `AnyValue.empty` (the otel equivalent of `null`).
  *   - A JSON boolean becomes `AnyValue.boolean`.
  *   - A JSON number becomes `AnyValue.long` when it is an exact integral `Long`, otherwise
  *     `AnyValue.double`. Values outside both ranges, or arbitrary-precision decimals, are narrowed to the
  *     nearest `Double`.
  *   - A JSON string becomes `AnyValue.string`.
  *   - A JSON array becomes `AnyValue.seq`, preserving element order.
  *   - A JSON object becomes `AnyValue.map`. Circe's key insertion order is not preserved, since the
  *     underlying `Map` is unordered.
  */
object JsonToAnyValue {
  def apply(json: Json): AnyValue = json.foldWith(folder)

  private val folder: Json.Folder[AnyValue] = new Json.Folder[AnyValue] {
    def onNull: AnyValue = AnyValue.empty
    def onBoolean(value: Boolean): AnyValue = AnyValue.boolean(value)
    def onString(value: String): AnyValue = AnyValue.string(value)

    def onNumber(value: JsonNumber): AnyValue =
      value.toLong match {
        case Some(l) => AnyValue.long(l)
        case None    => AnyValue.double(value.toDouble)
      }

    def onArray(value: Vector[Json]): AnyValue =
      AnyValue.seq(value.map(JsonToAnyValue.apply))

    def onObject(value: JsonObject): AnyValue =
      AnyValue.map(value.toMap.map { case (k, v) => k -> JsonToAnyValue.apply(v) })
  }
}
