package com.github.chenharryhua.nanjin.spark

import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.io.DataOutputStream
import java.util.UUID

package object persist {
  private[persist] def uuidStr(job: TaskAttemptContext): String =
    UUID.nameUUIDFromBytes(job.getTaskAttemptID.toString.getBytes).toString

  /** helper: safely close resource and rethrow exception */
  private[persist] def handleException(os: DataOutputStream)(ex: Throwable): Nothing = {
    try os.close()
    catch {
      case _: Throwable => ()
    }
    throw ex // scalafix:ok
  }
}
