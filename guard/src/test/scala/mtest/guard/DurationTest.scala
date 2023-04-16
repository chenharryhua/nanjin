package mtest.guard

import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.{Arbitrary, Gen, Properties}

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

class DurationTest extends Properties("durations") {
  implicit val gen: Arbitrary[Duration] = Arbitrary(Gen.choose(0, Long.MaxValue).map(Duration.ofNanos))
  property("nano") = forAll { (duration: Duration) =>
    FiniteDuration(duration.toNanos, TimeUnit.NANOSECONDS) == duration.toScala
  }
}
