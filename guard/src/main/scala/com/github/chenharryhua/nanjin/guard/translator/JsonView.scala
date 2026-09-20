package com.github.chenharryhua.nanjin.guard.translator

import cats.syntax.show.showInterpolator
import io.circe.{Json, JsonNumber, JsonObject}
import org.apache.commons.lang3.StringUtils

import java.text.DecimalFormat

/** A presentation-only view of a circe `Json` node, tagging it by its JSON shape.
  *
  * `JsonView` is an intermediate representation produced by folding a `Json` value with the `unfolded`
  * `Json.Folder`. It exists solely to drive the YAML-ish rendering in `yml`; it is not a general-purpose
  * serializer and carries no round-trip guarantees. The type parameter `A` is the element type held by the
  * container shapes (`ArrayView`, `ObjectView`); scalar shapes fix it to `Nothing`.
  *
  * @tparam A
  *   the type of nested children (`Json` in practice), or `Nothing` for scalar shapes
  */
sealed private trait JsonView[+A]

private object JsonView {

  final private case class NullView() extends JsonView[Nothing]
  final private case class BooleanView(bool: Boolean) extends JsonView[Nothing]
  final private case class NumberView(number: JsonNumber) extends JsonView[Nothing]
  final private case class StringView(str: String) extends JsonView[Nothing]
  final private case class ArrayView[A](values: List[A]) extends JsonView[A]
  final private case class ObjectView[A](fields: List[(String, A)]) extends JsonView[A]

  /** Classifies a `Json` node into its corresponding `JsonView` shape, keeping children as raw `Json` so
    * callers can fold one level deeper on demand.
    */
  private val unfolded: Json.Folder[JsonView[Json]] =
    new Json.Folder[JsonView[Json]] {
      def onNull: JsonView[Json] = NullView()
      def onBoolean(value: Boolean): JsonView[Json] = BooleanView(value)
      def onNumber(value: JsonNumber): JsonView[Json] = NumberView(value)
      def onString(value: String): JsonView[Json] = StringView(value)
      def onArray(value: Vector[Json]): JsonView[Json] = ArrayView(value.toList)
      def onObject(value: JsonObject): JsonView[Json] = ObjectView(value.toList)
    }

  private val decimalFormatter: DecimalFormat = new DecimalFormat(decimalFormat)

  /** Formats a JSON number with the shared `decimalFormat` grouping pattern, falling back to the number's own
    * string form when it has no `BigDecimal` representation.
    */
  private def format_json_number(jn: JsonNumber): String =
    jn.toBigDecimal.map(decimalFormatter.format).getOrElse(jn.toString)

  /** Renders a `Json` value as a list of YAML-ish lines under a given label.
    *
    * The output is a compact, human-readable view intended for metric and gauge display (see
    * `SnapshotPolyglot`), not a spec-compliant YAML serializer. Rendering goes at most one level deep: nested
    * containers inside an object are collapsed to a single line rather than expanded recursively.
    *
    * Behavior by top-level shape:
    *   - Null: renders to nothing (an empty list), so null-valued entries are omitted.
    *   - Boolean / number / string: a single `"$name: $value"` line. Numbers use `format_json_number`.
    *   - Array: a single inline line `"$name: [a, b, c]"`, each element in its compact JSON form.
    *   - Object: a `"$name:"` header line followed by one line per field, each indented by four `space`
    *     characters and with keys right-padded to the widest key for alignment. Field values render as
    *     scalars, arrays render inline, and nested objects collapse to compact single-line JSON.
    *
    * @param name
    *   the label used as the line prefix (and the object header)
    * @param json
    *   the JSON value to render
    * @param space
    *   the character used for both indentation and key-alignment padding
    * @return
    *   the rendered lines, or an empty list when `json` is null
    */
  def yml(name: String, json: Json, space: Char): List[String] = {
    val space4 = String.valueOf(space) * 4
    json.foldWith(unfolded) match {
      case NullView()         => Nil
      case BooleanView(bool)  => List(show"$name: $bool")
      case NumberView(number) => List(show"$name: ${format_json_number(number)}")
      case StringView(str)    => List(show"$name: $str")
      case ArrayView(values)  =>
        List(show"$name: ${values.map(_.noSpaces).mkString("[", ", ", "]")}")
      case ObjectView(fields) =>
        val maxKeyLength = fields.map(_._1.length).foldLeft(0)(math.max)
        val content: List[String] = fields.map { case (key, js) =>
          val jsStr: String = js.foldWith(unfolded) match {
            case NullView()         => "null"
            case BooleanView(bool)  => bool.toString
            case NumberView(number) => format_json_number(number)
            case StringView(str)    => str
            case ArrayView(values)  => values.map(_.noSpaces).mkString("[", ", ", "]")
            case ObjectView(fields) => Json.obj(fields*).noSpaces
          }
          // add 4 space
          show"$space4${StringUtils.rightPad(key, maxKeyLength, space)}: $jsStr"
        }

        // don't forget attach name
        s"$name:" :: content
    }
  }
}
