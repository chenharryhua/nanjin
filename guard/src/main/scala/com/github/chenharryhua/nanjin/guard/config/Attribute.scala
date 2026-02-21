package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import io.circe.{Encoder, Json}

import scala.reflect.runtime.universe.TypeTag

final case class TextEntry(tag: String, text: String) {
  def toPair: (String, String) = (tag, text)
}

final class Attribute[A] private (value: A, typeName: String) {
  private lazy val snakeName: String =
    typeName.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").toLowerCase

  private lazy val camelName: String = s"${typeName.head.toLower}${typeName.tail}"

  def entry[B](f: A => B): (String, B) = (typeName, f(value))

  def labelledText(implicit show: Show[A]): String = s"$typeName:${show.show(value)}"

  def snakeJsonEntry(implicit enc: Encoder[A]): (String, Json) = snakeName -> enc.apply(value)
  def camelJsonEntry(implicit enc: Encoder[A]): (String, Json) = camelName -> enc.apply(value)

  def textEntry(implicit show: Show[A]): TextEntry = TextEntry(typeName, show.show(value))

  def map[B](f: A => B): Attribute[B] = new Attribute[B](f(value), typeName)
}

object Attribute {
  def apply[A](a: A)(implicit tag: TypeTag[A]): Attribute[A] =
    new Attribute[A](a, tag.tpe.typeSymbol.name.toString)

  def apply[A](oa: Option[A])(implicit tag: TypeTag[A]): Attribute[Option[A]] =
    new Attribute[Option[A]](oa, tag.tpe.typeSymbol.name.toString)

  implicit val functorAttribute: Functor[Attribute] = new Functor[Attribute] {
    override def map[A, B](fa: Attribute[A])(f: A => B): Attribute[B] = fa.map(f)
  }
}
