package com.github.chenharryhua.nanjin.guard.translator

import cats.{Functor, Show}
import io.circe.{Encoder, Json}
import munit.FunSuite

final private case class OAuthToken(value: String)
object OAuthToken {
  given Show[OAuthToken] = Show.show(_.value)
  given Encoder[OAuthToken] = Encoder.encodeString.contramap(_.value)
}

class LabelledTest extends FunSuite {

  test("1.TextEntry exposes its pair and transforms tag and text independently") {
    val entry = TextEntry("Tag", "value")

    assertEquals(entry.toPair, "Tag" -> "value")
    assertEquals(entry.withTag(_.toLowerCase), TextEntry("tag", "value"))
    assertEquals(entry.withText(_.toUpperCase), TextEntry("Tag", "VALUE"))
  }

  test("2.Labelled derives stable acronym-aware snake and camel JSON keys") {
    val labelled = Labelled(OAuthToken("secret"))

    assertEquals(labelled.typeName, "OAuthToken")
    assertEquals(labelled.snakeJsonEntry, "o_auth_token" -> Json.fromString("secret"))
    assertEquals(labelled.camelJsonEntry, "oAuthToken" -> Json.fromString("secret"))
  }

  test("3.Labelled renders text, arbitrary entries, and folds over its label and value") {
    val labelled = Labelled(OAuthToken("secret"))

    assertEquals(labelled.labelledText, "OAuthToken:secret")
    assertEquals(labelled.textEntry, TextEntry("OAuthToken", "secret"))
    assertEquals(labelled.entry(_.value.length), "OAuthToken" -> 6)
    assertEquals(labelled.fold((name, token) => s"$name=${token.value}"), "OAuthToken=secret")
  }

  test("4.map and the Functor instance preserve the original type label") {
    val labelled = Labelled(OAuthToken("secret"))
    val mapped = labelled.map(_.value.length)
    val functor_mapped = Functor[Labelled].map(labelled)(_.value.reverse)

    assertEquals(mapped.typeName, "OAuthToken")
    assertEquals(mapped.fold((_, value) => value), 6)
    assertEquals(functor_mapped.typeName, "OAuthToken")
    assertEquals(functor_mapped.fold((_, value) => value), "terces")
  }

  test("5.Option values retain the element type label rather than Option") {
    val present = Labelled(Option(OAuthToken("secret")))
    val absent = Labelled(Option.empty[OAuthToken])

    assertEquals(present.typeName, "OAuthToken")
    assertEquals(absent.typeName, "OAuthToken")
    assertEquals(present.snakeJsonEntry._1, "o_auth_token")
    assertEquals(absent.snakeJsonEntry._2, Json.Null)
  }
}
