package com.github.chenharryhua.nanjin.guard.translator

import cats.implicits.showInterpolator
import io.circe.{Json, JsonNumber, JsonObject}
import org.apache.commons.lang3.StringUtils

sealed trait JsonView[+A]

object JsonView {

  final private case class NullView() extends JsonView[Nothing]
  final private case class BooleanView(bool: Boolean) extends JsonView[Nothing]
  final private case class NumberView(number: JsonNumber) extends JsonView[Nothing]
  final private case class StringView(str: String) extends JsonView[Nothing]
  final private case class ArrayView[A](values: List[A]) extends JsonView[A]
  final private case class ObjectView[A](fields: List[(String, A)]) extends JsonView[A]

  private val unfolded: Json.Folder[JsonView[Json]] =
    new Json.Folder[JsonView[Json]] {
      def onNull: JsonView[Json] = NullView()
      def onBoolean(value: Boolean): JsonView[Json] = BooleanView(value)
      def onNumber(value: JsonNumber): JsonView[Json] = NumberView(value)
      def onString(value: String): JsonView[Json] = StringView(value)
      def onArray(value: Vector[Json]): JsonView[Json] = ArrayView(value.toList)
      def onObject(value: JsonObject): JsonView[Json] = ObjectView(value.toList)
    }

  private def format_json_umber(jn: JsonNumber): String =
    jn.toBigDecimal.map(decimalFormatter.format).getOrElse(jn.toString)

  def yml(name: String, json: Json): List[String] =
    json.foldWith(unfolded) match {
      case NullView()         => Nil
      case BooleanView(bool)  => List(show"$name: $bool")
      case NumberView(number) => List(show"$name: ${format_json_umber(number)}")
      case StringView(str)    => List(show"$name: $str")
      case ArrayView(values)  =>
        List(show"$name: ${values.map(_.noSpaces).mkString("[", ", ", "]")}")
      case ObjectView(fields) =>
        val maxKeyLength = fields.map(_._1.length).foldLeft(0)(math.max)
        val content: List[String] = fields.map { case (key, js) =>
          val jsStr: String = js.foldWith(unfolded) match {
            case NullView()         => "null"
            case BooleanView(bool)  => bool.toString
            case NumberView(number) => format_json_umber(number)
            case StringView(str)    => str
            case ArrayView(values)  => values.map(_.noSpaces).mkString("[", ", ", "]")
            case ObjectView(fields) => Json.obj(fields*).noSpaces
          }
          // add 4 space
          show"$space4${StringUtils.rightPad(key, maxKeyLength)}: $jsStr"
        }

        // don't forget attach name
        s"$name:" :: content
    }
}
