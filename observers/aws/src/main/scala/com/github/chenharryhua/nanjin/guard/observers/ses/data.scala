package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.effect.kernel.Resource
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.aws.SimpleEmailService
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.Capacity
import com.github.chenharryhua.nanjin.guard.translator.Translator
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZoneId

final private case class ColoredTag(tag: Text.TypedTag[String], color: LogLevel)

final private case class Letter(
  warns: Int,
  errors: Int,
  notice: Text.TypedTag[String],
  content: List[Text.TypedTag[String]]) {
  private val email_header: Text.TypedTag[String] =
    head(tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 90%;
        }
      """))

  def emailBody(capacity: Capacity): String = {
    val foot = footer(hr(p(b("Events/Max: "), show"${content.size}/$capacity")))
    html(email_header, body(notice, content, foot)).render
  }
}

final private case class Params[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  translator: Translator[F, Text.TypedTag[String]],
  isNewestFirst: Boolean,
  capacity: Capacity,
  policy: Policy.type => Policy,
  zoneId: ZoneId)
