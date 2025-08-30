package com.github.chenharryhua.nanjin.spark

import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.UUID

package object persist {
  private[persist] def uuidStr(job: TaskAttemptContext): String =
    UUID.nameUUIDFromBytes(job.getTaskAttemptID.getJobID.toString.getBytes).toString
}
