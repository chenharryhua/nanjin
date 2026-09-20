package com.github.chenharryhua.nanjin.common.logging

import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Decoder, Encoder}

import java.time.Instant

/** A URL that points at a service's logs, typically a deep link into a log-viewer console (e.g. AWS
  * CloudWatch Logs). The wrapped `String` is the fully-formed link; obtain it with `value`.
  *
  * The type is opaque so a raw `String` cannot be passed where a link is expected, and vice versa. It carries
  * `Encoder`/`Decoder` instances so a link can be serialized on an event (see the guard's event model), where
  * it appears as the underlying JSON string.
  */
opaque type LogLink = String
object LogLink:
  def apply(str: String): LogLink = str

  extension (ll: LogLink) inline def value: String = ll

  given Encoder[LogLink] = OpaqueLift.lift[LogLink, String, Encoder]
  given Decoder[LogLink] = OpaqueLift.lift[LogLink, String, Decoder]
end LogLink

/** Produces a `LogLink` for a given instant.
  *
  * The abstraction lets the core (which knows nothing about any particular log backend) obtain a deep link to
  * the logs surrounding an event, while the concrete backend (e.g. `CloudWatchLogLocator` in the aws module)
  * owns the details of URL construction and any time window placed around `timestamp`. A locator is expected
  * to be built once for a running service and reused for every event.
  *
  * @see
  *   the aws module's `CloudWatchLogLocator` for the CloudWatch console implementation
  */
trait LogLocator {

  /** Build a link to the logs at `timestamp`. Implementations may widen `timestamp` into a surrounding time
    * window so the link opens the viewer focused on the entries around that moment.
    */
  def locate(timestamp: Instant): LogLink
}
