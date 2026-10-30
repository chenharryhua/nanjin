package mtest.terminals

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.terminals.{FolderRetentionResult, RetentionStatus}
import io.circe.Json
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import munit.FunSuite

class RetentionStatusTest extends FunSuite {

  private val CASES = List(
    RetentionStatus.Removed -> "Removed",
    RetentionStatus.RemoveFailed -> "RemoveFailed",
    RetentionStatus.Retained -> "Retained")

  test("1.every RetentionStatus has a stable JSON string and Show representation") {
    assertEquals(CASES.map(_._1), RetentionStatus.values.toList)
    CASES.foreach { case (status, serialized) =>
      assertEquals(status.asJson, Json.fromString(serialized))
      assertEquals(status.show, serialized)
    }
  }

  test("2.every RetentionStatus decodes and round-trips") {
    CASES.foreach { case (status, serialized) =>
      assertEquals(Json.fromString(serialized).as[RetentionStatus].toOption, Some(status))
      assertEquals(status.asJson.as[RetentionStatus].toOption, Some(status))
    }
  }

  test("3.unknown, empty, and case-mismatched values are rejected with the input in the error") {
    List("", "removed", "Unknown", "Remove-Failed").foreach { input =>
      val failure = Json.fromString(input).as[RetentionStatus].left.toOption
      assertEquals(failure.map(_.message), Some(s"Invalid RetentionStatus: $input"))
    }
  }

  test("4.FolderRetentionResult preserves its URL and status through JSON") {
    CASES.foreach { case (status, _) =>
      val result = FolderRetentionResult(Url.parse("s3a://bucket/archive/Year=2026"), status)
      assertEquals(result.asJson.as[FolderRetentionResult].toOption, Some(result))
    }
  }

  test("5.serialized values are unique") {
    assertEquals(CASES.map(_._2).distinct.size, RetentionStatus.values.length)
  }
}
