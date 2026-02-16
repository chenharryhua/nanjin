package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.guard.translator.ColorScheme
import scalatags.Text
import scalatags.Text.all.*

final private case class ColoredTag(tag: Text.TypedTag[String], color: ColorScheme)

final private case class Letter(
  warns: Int,
  errors: Int,
  notice: Text.TypedTag[String],
  content: List[Text.TypedTag[String]]) {
  private val emailHeader: Text.TypedTag[String] =
    head(tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 90%;
        }
      """))

  def emailBody(chunkSize: ChunkSize): String = {
    val foot = footer(hr(p(b("Events/Max: "), show"${content.size}/${chunkSize.value}")))
    html(emailHeader, body(notice, content, foot)).render
  }
}
