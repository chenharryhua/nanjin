package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import io.circe.{Encoder, Json}

import scala.reflect.runtime.universe.TypeTag

final case class TextEntry(tag: String, text: String) {
  def toPair: (String, String) = (tag, text)
}

final class Attribute[A] private (value: A, textValue: String, name: String) {
  private def snakeName: String =
    name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").toLowerCase

  private def camelName: String = s"${name.head.toLower}${name.tail}"

  def labelledText: String = s"$name:$textValue"

  def snakeJsonEntry(implicit encoder: Encoder[A]): (String, Json) = snakeName -> encoder.apply(value)
  def camelJsonEntry(implicit encoder: Encoder[A]): (String, Json) = camelName -> encoder.apply(value)

  def textEntry: TextEntry = TextEntry(name, textValue)
}

object Attribute {
  def apply[A: Show](a: A)(implicit tag: TypeTag[A]): Attribute[A] = {
    val name = tag.tpe.typeSymbol.name.toString
    new Attribute[A](a, Show[A].show(a), name)
  }
}
