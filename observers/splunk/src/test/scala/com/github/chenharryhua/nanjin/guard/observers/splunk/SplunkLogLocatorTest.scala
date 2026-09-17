package com.github.chenharryhua.nanjin.guard.observers.splunk

import org.http4s.Uri
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.DurationInt

class SplunkLogLocatorTest extends AnyFunSuite {

  private val webBase: Uri = Uri.unsafeFromString("https://splunk.example.com:8000")

  test("1.locate builds a search-app link with an epoch-second window") {
    val locator = SplunkLogLocator(webBase, "index=main source=nanjin", 30.seconds)
    val at = Instant.ofEpochSecond(1_000_000L)
    val link = Uri.unsafeFromString(locator.locate(at).value)

    // scheme and authority of webBase are preserved
    assert(link.scheme.map(_.value).contains("https"))
    assert(link.host.map(_.value).contains("splunk.example.com"))
    assert(link.port.contains(8000))
    assert(link.path.renderString == "/en-US/app/search/search")
    // window is +/- 30s around the event
    assert(link.query.params.get("earliest").contains("999970"))
    assert(link.query.params.get("latest").contains("1000030"))
    // the parsed query decodes the SPL back to plain text
    assert(link.query.params.get("q").contains("index=main source=nanjin"))
    // all three query params are present together, none clobbered
    assert(link.query.params.keySet == Set("q", "earliest", "latest"))
  }

  test("2.the SPL query is URL-encoded in the rendered link") {
    val locator = SplunkLogLocator(webBase, "index=main source=nanjin", 30.seconds)
    val rendered = locator.locate(Instant.EPOCH).value
    // spaces and '=' inside the SPL must be percent-encoded so they don't break the query string
    assert(rendered.contains("q=index%3Dmain%20source%3Dnanjin"))
  }

  test("3.window straddling the epoch yields a negative earliest") {
    // documents current behavior: the window is computed as plain epoch-second arithmetic,
    // so an event within `window` of 1970-01-01 produces a negative earliest.
    val locator = SplunkLogLocator(webBase, "index=main", 30.seconds)
    val link = Uri.unsafeFromString(locator.locate(Instant.EPOCH).value)
    assert(link.query.params.get("earliest").contains("-30"))
    assert(link.query.params.get("latest").contains("30"))
  }

  test("4.window width follows the configured duration") {
    val locator = SplunkLogLocator(webBase, "index=main", 5.minutes)
    val at = Instant.ofEpochSecond(10_000_000L)
    val link = Uri.unsafeFromString(locator.locate(at).value)
    assert(link.query.params.get("earliest").contains((10_000_000L - 300).toString))
    assert(link.query.params.get("latest").contains((10_000_000L + 300).toString))
  }

  test("5.a custom app name is used in the search path") {
    val locator = SplunkLogLocator(webBase, "index=main", 30.seconds, app = "my_app")
    val link = Uri.unsafeFromString(locator.locate(Instant.EPOCH).value)
    assert(link.path.renderString == "/en-US/app/my_app/search")
  }

  test("6.any path on webBase is replaced by the search-app path") {
    val withPath: Uri = Uri.unsafeFromString("https://splunk.example.com:8000/leftover/path")
    val locator = SplunkLogLocator(withPath, "index=main", 30.seconds)
    val link = Uri.unsafeFromString(locator.locate(Instant.EPOCH).value)
    assert(link.path.renderString == "/en-US/app/search/search")
  }
}
