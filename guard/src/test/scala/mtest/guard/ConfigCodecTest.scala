package mtest.guard

import com.github.chenharryhua.nanjin.guard.config.{
  Capacity,
  Homepage,
  LaunchTime,
  LogThreshold,
  Timestamp,
  UpTime
}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import io.circe.syntax.EncoderOps
import io.circe.jawn.decode
import io.circe.{Decoder, Encoder}
import munit.FunSuite
import org.http4s.Uri

import java.time.{Duration, ZoneId, ZonedDateTime}

/** Codec round-trip coverage for the config opaque types (report item NV cluster).
  *
  * These opaque types are serialized into every event payload but had no dedicated round-trip test. A
  * `decode(encode(a)) == a` failure would silently corrupt the wire output, so pin each here. Production
  * source is not modified by these tests.
  */
class ConfigCodecTest extends FunSuite {

  private def roundTrip[A: Encoder: Decoder](a: A): Either[io.circe.Error, A] =
    decode[A](a.asJson.noSpaces)

  // ---------------- Homepage ----------------

  test("1.Homepage round-trips at the Uri level for a canonical URL") {
    val hp = Homepage("https://example.com/path")
    assertEquals(roundTrip(hp).map(_.uri), Right(hp.uri))
  }

  test("2.Homepage serializes to its canonical rendering (normalization is intentional)") {
    // a default https port is dropped by http4s canonical rendering; the serialized string is the
    // normalized form, not the raw input. This documents the known string-vs-raw divergence (M1).
    val hp = Homepage("https://example.com:443/a")
    val wire = hp.asJson.as[String].toOption.get
    // round-trips to the same Uri regardless of the normalization
    assertEquals(roundTrip(hp).map(_.uri), Right(hp.uri))
    // and the wire form equals the Uri's own renderString (canonical), which is what `value` returns
    assertEquals(wire, hp.value)
  }

  test("3.Homepage decoder rejects a non-URI string with a Left, not a throw") {
    val bad = io.circe.Json.fromString("::::not a uri::::").noSpaces
    val result = decode[Homepage](bad)
    assert(result.isLeft, s"expected Left for invalid URI, got $result")
  }

  test("4.Homepage(Uri) is total and agrees with Homepage(String) for a canonical URL") {
    val uri = Uri.unsafeFromString("https://example.com/x")
    assertEquals(Homepage(uri).uri, Homepage("https://example.com/x").uri)
  }

  // ---------------- Timestamp / LaunchTime (zone preservation) ----------------

  test("5.Timestamp round-trips preserving the zone") {
    val zdt = ZonedDateTime.of(2024, 6, 1, 12, 30, 0, 0, ZoneId.of("Australia/Sydney"))
    val ts = Timestamp(zdt)
    assertEquals(roundTrip(ts).map(_.value), Right(ts.value))
    assertEquals(roundTrip(ts).map(_.value.getZone), Right(ZoneId.of("Australia/Sydney")))
  }

  test("6.LaunchTime round-trips preserving the zone") {
    val zdt = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("America/New_York"))
    val lt = LaunchTime(zdt)
    assertEquals(roundTrip(lt).map(_.zoned), Right(lt.zoned))
    assertEquals(roundTrip(lt).map(_.zoneId), Right(ZoneId.of("America/New_York")))
  }

  test("7.LaunchTime.upTime yields the duration between launch and a later timestamp") {
    val launch = LaunchTime(ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC")))
    val ts = Timestamp(ZonedDateTime.of(2024, 1, 1, 0, 1, 30, 0, ZoneId.of("Etc/UTC")))
    assertEquals(launch.upTime(ts).value, Duration.ofSeconds(90))
  }

  // ---------------- UpTime / Capacity / LogThreshold ----------------

  test("8.UpTime round-trips") {
    val up = UpTime(Duration.ofSeconds(3661))
    assertEquals(roundTrip(up).map(_.value), Right(up.value))
  }

  test("9.Capacity round-trips, including non-positive (disable) sentinels") {
    assertEquals(roundTrip(Capacity(32)).map(_.value), Right(32))
    assertEquals(roundTrip(Capacity(0)).map(_.value), Right(0))
    assertEquals(roundTrip(Capacity(-5)).map(_.value), Right(-5))
  }

  test("10.LogThreshold round-trips and min is the lower of the two levels") {
    val lt = LogThreshold(logger = LogLevel.Debug, channel = LogLevel.Warn)
    assertEquals(roundTrip(lt), Right(lt))
    assertEquals(lt.min, LogLevel.Debug)
  }
}
