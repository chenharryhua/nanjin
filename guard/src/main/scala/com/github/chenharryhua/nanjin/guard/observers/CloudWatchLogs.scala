package com.github.chenharryhua.nanjin.guard.observers

import cats.syntax.foldable.given
import com.github.chenharryhua.nanjin.guard.config.Brief

import java.time.Instant

/** Extracts CloudWatch Logs configuration from the service brief's LogOptions and builds a console URL scoped
  * to the specific log stream with a time window.
  *
  * Expected brief structure (from ECS task definition):
  * {{{
  * {
  *   "LogOptions": {
  *     "awslogs-group": "/ecs/my-service",
  *     "awslogs-region": "ap-southeast-2",
  *     "awslogs-stream": "ecs/container/task-id"
  *   }
  * }
  * }}}
  */
object CloudWatchLogs {

  final private case class LogOptions(logGroup: String, region: String, logStream: String)

  private def extract(brief: Brief): Option[LogOptions] = {
    val cursor = brief.value.hcursor
    // search in the top-level array (brief is a JSON array of config objects)
    brief.value.asArray
      .flatMap { arr =>
        arr.collectFirstSome { json =>
          val c = json.hcursor.downField("LogOptions")
          for {
            group <- c.get[String]("awslogs-group").toOption
            region <- c.get[String]("awslogs-region").toOption
            stream <- c.get[String]("awslogs-stream").toOption
          } yield LogOptions(group, region, stream)
        }
      }
      .orElse {
        // fallback: brief itself might be a single object
        val c = cursor.downField("LogOptions")
        for {
          group <- c.get[String]("awslogs-group").toOption
          region <- c.get[String]("awslogs-region").toOption
          stream <- c.get[String]("awslogs-stream").toOption
        } yield LogOptions(group, region, stream)
      }
  }

  // CloudWatch console uses double URL-encoding: / -> %2F -> $252F
  private def encode(s: String): String =
    s.replace("/", "$252F").replace(":", "$253A")

  /** Build a CloudWatch Logs console URL scoped to the specific log stream with a ±30 second time window
    * around the event.
    *
    * @param brief
    *   service brief containing LogOptions
    * @param timestamp
    *   event timestamp used to compute the time window
    * @return
    *   None if LogOptions is absent from the brief
    */
  def logLink(brief: Brief, timestamp: Instant): Option[String] =
    extract(brief).map { opts =>
      val encodedGroup = encode(opts.logGroup)
      val encodedStream = encode(opts.logStream)
      val start = timestamp.minusSeconds(30).toEpochMilli
      val end = timestamp.plusSeconds(30).toEpochMilli
      s"https://${opts.region}.console.aws.amazon.com/cloudwatch/home?region=${opts.region}" +
        s"#logsV2:log-groups/log-group/$encodedGroup/log-events/$encodedStream" +
        s"$$3Fstart$$3D$start$$26end$$3D$end"
    }
}
