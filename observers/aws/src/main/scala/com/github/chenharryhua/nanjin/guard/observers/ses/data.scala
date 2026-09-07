package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.Capacity
import scalatags.Text
import scalatags.Text.all.*

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
