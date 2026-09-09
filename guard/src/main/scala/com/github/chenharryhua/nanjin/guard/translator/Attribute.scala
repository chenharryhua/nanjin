package com.github.chenharryhua.nanjin.guard.translator

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.TypeName
import io.circe.{Encoder, Json}

import java.util.regex.Pattern
import scala.annotation.publicInBinary
import scala.util.chaining.given

/** A rendered label/value pair.
  *
  * Produced by `Attribute.textEntry`, where `tag` is the human-facing label (the value's type name) and
  * `text` is the `Show`-rendered value. Consumed by the observer translators (Slack, Teams, SES/HTML) to
  * build fields, rows, and sections.
  *
  * @param tag
  *   the label, i.e. the originating value's type name
  * @param text
  *   the rendered value
  */
final case class TextEntry(tag: String, text: String) {

  /** This entry as a plain `(tag, text)` tuple. */
  def toPair: (String, String) = (tag, text)
}

/** Pairs a value with a label derived from its static type, and renders that pair in the shapes the observer
  * translators need.
  *
  * An `Attribute` is built via `Attribute.apply`, which captures the value together with its `typeName` (from
  * the `TypeName[A]` instance). From that single label the various accessors derive the forms callers want: a
  * text entry, a JSON entry keyed in snake_case or camelCase, a labelled string, or a raw `(label, value)`
  * tuple. It is the shared building block behind the CloudWatch, Slack, Teams, and SES/HTML translators.
  *
  * @tparam A
  *   the wrapped value type
  * @param value
  *   the wrapped value
  * @param typeName
  *   the label for the value, taken from `TypeName[A]`
  */
final class Attribute[A] @publicInBinary private (value: A, val typeName: String) {

  /** `typeName` converted to `snake_case` (used as a JSON key by `snakeJsonEntry`). */
  private lazy val snakeName: String =
    Attribute.camelToSnake1.matcher(typeName).replaceAll("$1_$2").pipe(s =>
      Attribute.camelToSnake2.matcher(s).replaceAll("$1_$2").toLowerCase)

  /** `typeName` with its first letter lowercased, i.e. `camelCase` (used as a JSON key by `camelJsonEntry`).
    */
  private lazy val camelName: String = s"${typeName.head.toLower}${typeName.tail}"

  /** Pairs the `typeName` label with an arbitrary projection of the value. */
  def entry[B](f: A => B): (String, B) = (typeName, f(value))

  /** The value rendered as `"$typeName:$shown"` using its `Show` instance. */
  def labelledText(using show: Show[A]): String = s"$typeName:${show.show(value)}"

  /** A JSON entry whose key is the `snake_case` type name and whose value is the encoded value. */
  def snakeJsonEntry(using enc: Encoder[A]): (String, Json) = snakeName -> enc.apply(value)

  /** A JSON entry whose key is the `camelCase` type name and whose value is the encoded value. */
  def camelJsonEntry(using enc: Encoder[A]): (String, Json) = camelName -> enc.apply(value)

  /** A `TextEntry` pairing the `typeName` label with the `Show`-rendered value. */
  def textEntry(using show: Show[A]): TextEntry = TextEntry(typeName, show.show(value))

  /** Transforms the wrapped value while keeping the original `typeName` label. */
  def map[B](f: A => B): Attribute[B] = new Attribute[B](f(value), typeName)

  /** Applies `f` to the label and the wrapped value together. */
  def fold[B](f: (String, A) => B): B = f(typeName, value)
}

object Attribute:
  /** Matches a lowercase/digit followed by an uppercase letter, for camel-to-snake conversion. */
  private val camelToSnake1: Pattern = Pattern.compile("([a-z0-9])([A-Z])")

  /** Matches an uppercase run followed by an uppercase-then-lowercase, splitting acronym boundaries during
    * camel-to-snake conversion.
    */
  private val camelToSnake2: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")

  /** Wraps an `Option` value, labelling it with the element type's name (not `Option`'s). */
  def apply[A](oa: Option[A])(using tn: TypeName[A]): Attribute[Option[A]] =
    new Attribute(oa, tn.value)

  /** Wraps a value, labelling it with its type name from `TypeName[A]`. */
  def apply[A](a: A)(using tn: TypeName[A]): Attribute[A] =
    new Attribute[A](a, tn.value)

  /** Maps over the wrapped value while preserving the label; see `Attribute.map`. */
  given Functor[Attribute] = new Functor[Attribute] {
    override def map[A, B](fa: Attribute[A])(f: A => B): Attribute[B] = fa.map(f)
  }
