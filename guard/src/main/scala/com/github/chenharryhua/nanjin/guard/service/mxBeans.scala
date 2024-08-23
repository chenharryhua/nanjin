package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.syntax.all.*

import java.lang.management.*
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object mxBeans {
  val classloader: Eval[ClassLoadGauge] =
    Eval.always {
      val mxBean: ClassLoadingMXBean = ManagementFactory.getClassLoadingMXBean
      ClassLoadGauge(
        loaded = mxBean.getLoadedClassCount,
        unloaded = mxBean.getUnloadedClassCount,
        total = mxBean.getTotalLoadedClassCount)
    }

  val deadlocks: Eval[List[ThreadDeadlocks]] =
    Eval.always {
      val mxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
      Option(mxBean.findDeadlockedThreads()).traverse {
        _.toList.map { id =>
          val info: ThreadInfo = mxBean.getThreadInfo(id, 100)
          ThreadDeadlocks(
            thread = info.getThreadName,
            lock = Option(info.getLockName),
            owner = Option(info.getLockOwnerName),
            stack = info.getStackTrace.map(_.toString).toList
          )
        }
      }.flatten
    }

  val heapMemory: Eval[HeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    HeapMemory(
      init = mxBean.getHeapMemoryUsage.getInit,
      used = mxBean.getHeapMemoryUsage.getUsed,
      max = mxBean.getHeapMemoryUsage.getMax,
      committed = mxBean.getHeapMemoryUsage.getCommitted
    )
  }

  val nonHeapMemory: Eval[NonHeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    NonHeapMemory(
      init = mxBean.getNonHeapMemoryUsage.getInit,
      used = mxBean.getNonHeapMemoryUsage.getUsed,
      max = mxBean.getNonHeapMemoryUsage.getMax,
      committed = mxBean.getNonHeapMemoryUsage.getCommitted
    )
  }

  val garbageCollectors: Eval[List[GarbageCollector]] = Eval.always(
    ManagementFactory.getGarbageCollectorMXBeans.asScala.toList.map(gc =>
      GarbageCollector(
        name = gc.getName,
        count = gc.getCollectionCount,
        took = gc.getCollectionTime.milliseconds.toJava)))

  val threadState: Eval[ThreadState] = Eval.always {
    val mxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
    ThreadState(
      live = mxBean.getThreadCount,
      daemon = mxBean.getDaemonThreadCount,
      peak = mxBean.getPeakThreadCount,
      started = mxBean.getTotalStartedThreadCount
    )
  }

  private val operationSystem: Eval[OperationSystem] = Eval.always {
    val mxBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
    OperationSystem(
      architecture = mxBean.getArch,
      available_processors = mxBean.getAvailableProcessors,
      name = mxBean.getName,
      version = mxBean.getVersion,
      system_load_average = mxBean.getSystemLoadAverage
    )
  }

  val allJvmGauge: Eval[AllJvmGauge] =
    for {
      cl <- classloader
      dl <- deadlocks
      gc <- garbageCollectors
      hp <- heapMemory
      nh <- nonHeapMemory
      ts <- threadState
      os <- operationSystem
    } yield AllJvmGauge(
      classloader = cl,
      deadlocks = dl,
      garbage_collectors = gc,
      heap_memory = hp,
      non_heap_memory = nh,
      thread_state = ts,
      operation_system = os
    )
}
