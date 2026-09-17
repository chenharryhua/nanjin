package com.github.chenharryhua.nanjin.guard.observers.splunk

import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import org.http4s.Uri

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** A `LogLocator` that deep-links into the Splunk Web search app.
  *
  * Unlike the CloudWatch locator, Splunk exposes no runtime metadata endpoint to discover deployment details,
  * so everything the link needs is supplied at construction: the Splunk Web base URL, the SPL search that
  * scopes the logs, and the half-width of the time window placed around each event.
  *
  * `locate` produces a link of the form
  * {{{
  * https://<host>/en-US/app/<app>/search?q=<spl>&earliest=<epoch>&latest=<epoch>
  * }}}
  * where `earliest`/`latest` are Unix epoch seconds `window` before/after the event timestamp. The `q` and
  * time parameters are URL-encoded by the underlying `org.http4s.Uri`, so the caller passes plain SPL (e.g.
  * `index=main source=nanjin`) without escaping it.
  *
  * Note: the Splunk Web UI (typically port 8000) is a different surface from the HTTP Event Collector
  * endpoint used by `SplunkObserver` (typically port 8088); `webBase` here is the browsable UI host, not the
  * HEC URI.
  */
final class SplunkLogLocator private (webBase: Uri, search: String, app: String, window: FiniteDuration)
    extends LogLocator {

  override def locate(timestamp: Instant): LogLink = {
    val earliest = timestamp.minusSeconds(window.toSeconds).getEpochSecond
    val latest = timestamp.plusSeconds(window.toSeconds).getEpochSecond
    val uri = webBase
      .withPath(Uri.Path.unsafeFromString(s"/en-US/app/$app/search"))
      .withQueryParam("q", search)
      .withQueryParam("earliest", earliest.toString)
      .withQueryParam("latest", latest.toString)
    LogLink(uri.renderString)
  }
}

object SplunkLogLocator {

  /** Build a Splunk search-app log locator.
    *
    * @param webBase
    *   the Splunk Web base URL, e.g. `https://splunk.example.com:8000` (scheme and authority only; any path
    *   is replaced by the search-app path)
    * @param search
    *   the SPL query scoping the logs, e.g. `index=main source=nanjin` (passed as plain text; it is
    *   URL-encoded when the link is rendered)
    * @param window
    *   half-width of the time range placed around each event timestamp
    * @param app
    *   the Splunk app that owns the search view; defaults to the built-in `search` app
    */
  def apply(webBase: Uri, search: String, window: FiniteDuration, app: String = "search"): SplunkLogLocator =
    new SplunkLogLocator(webBase, search, app, window)
}
