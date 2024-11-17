package com.github.chenharryhua.nanjin.guard.translator

import cats.implicits.showInterpolator
import io.circe.{Json, JsonNumber, JsonObject}
import org.apache.commons.lang3.StringUtils

sealed trait JsonF[+A]

object JsonF {

  final private case object NullF extends JsonF[Nothing]
  final private case class BooleanF(bool: Boolean) extends JsonF[Nothing]
  final private case class NumberF(number: JsonNumber) extends JsonF[Nothing]
  final private case class StringF(str: String) extends JsonF[Nothing]
  final private case class ArrayF[A](values: List[A]) extends JsonF[A]
  final private case class ObjectF[A](fields: List[(String, A)]) extends JsonF[A]

  private val unfolded: Json.Folder[JsonF[Json]] =
    new Json.Folder[JsonF[Json]] {
      def onNull: JsonF[Json]                       = NullF
      def onBoolean(value: Boolean): JsonF[Json]    = BooleanF(value)
      def onNumber(value: JsonNumber): JsonF[Json]  = NumberF(value)
      def onString(value: String): JsonF[Json]      = StringF(value)
      def onArray(value: Vector[Json]): JsonF[Json] = ArrayF(value.toList)
      def onObject(value: JsonObject): JsonF[Json]  = ObjectF(value.toList)
    }

  def yml(name: String, json: Json): List[String] =
    json.foldWith(unfolded) match {
      case NullF           => Nil
      case BooleanF(bool)  => List(show"$name: $bool")
      case NumberF(number) => List(show"$name: ${number.toString}")
      case StringF(str)    => List(show"$name: $str")
      case ArrayF(values) =>
        List(show"$name: ${values.map(_.noSpaces).mkString("[", ", ", "]")}")
      case ObjectF(fields) =>
        val maxKeyLength = fields.map(_._1.length).max
        val content: List[String] = fields.map { case (key, js) =>
          val jsStr: String = js.foldWith(unfolded) match {
            case NullF           => "null"
            case BooleanF(bool)  => bool.toString
            case NumberF(number) => number.toString
            case StringF(str)    => str
            case ArrayF(values)  => values.map(_.noSpaces).mkString("[", ", ", "]")
            case ObjectF(fields) => Json.obj(fields*).noSpaces
          }
          // add 2 space
          show"  ${StringUtils.rightPad(key, maxKeyLength)}: $jsStr"
        }
        s"$name:" :: content
    }
}
