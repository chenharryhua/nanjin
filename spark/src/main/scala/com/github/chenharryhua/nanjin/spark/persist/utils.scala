package com.github.chenharryhua.nanjin.spark.persist

import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.UUID

private[persist] object utils {

  def uuidStr(job: TaskAttemptContext): String =
    UUID.nameUUIDFromBytes(job.getTaskAttemptID.getJobID.toString.getBytes).toString
}
