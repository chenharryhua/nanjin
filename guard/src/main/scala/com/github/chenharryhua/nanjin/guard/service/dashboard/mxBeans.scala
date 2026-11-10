package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.Eval
import cats.syntax.traverse.given

import java.lang.management.*
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object mxBeans {
  private val classloader: Eval[ClassLoadGauge] =
    Eval.always {
      val mxBean: ClassLoadingMXBean = ManagementFactory.getClassLoadingMXBean
      ClassLoadGauge(
        loaded = mxBean.getLoadedClassCount,
        unloaded = mxBean.getUnloadedClassCount,
        total = mxBean.getTotalLoadedClassCount)
    }

  private val deadlocks: Eval[List[ThreadDeadlocks]] =
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

  // Read the usage ONCE and project the four fields off it. Every `getHeapMemoryUsage` call is a fresh JMX
  // snapshot, so calling it per field would report four different instants and the figures could not be
  // compared with each other -- across a GC that can render `used` above `committed`.
  private val heap_memory: Eval[HeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    val usage: MemoryUsage = mxBean.getHeapMemoryUsage
    HeapMemory(
      init = usage.getInit,
      used = usage.getUsed,
      max = usage.getMax,
      committed = usage.getCommitted
    )
  }

  // Same single-read discipline as `heap_memory` above.
  private val non_heap_memory: Eval[NonHeapMemory] = Eval.always {
    val mxBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    val usage: MemoryUsage = mxBean.getNonHeapMemoryUsage
    NonHeapMemory(
      init = usage.getInit,
      used = usage.getUsed,
      max = usage.getMax,
      committed = usage.getCommitted
    )
  }

  private val garbage_collectors: Eval[List[GarbageCollector]] = Eval.always(
    ManagementFactory.getGarbageCollectorMXBeans.asScala.toList.map(gc =>
      GarbageCollector(
        name = gc.getName,
        count = gc.getCollectionCount,
        took = gc.getCollectionTime.milliseconds.toJava)))

  private val thread_state: Eval[ThreadState] = Eval.always {
    val mxBean: ThreadMXBean = ManagementFactory.getThreadMXBean
    ThreadState(
      live = mxBean.getThreadCount,
      daemon = mxBean.getDaemonThreadCount,
      peak = mxBean.getPeakThreadCount,
      started = mxBean.getTotalStartedThreadCount
    )
  }

  private val operating_system: Eval[OperatingSystem] = Eval.always {
    val mxBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
    OperatingSystem(
      architecture = s"${mxBean.getArch} ${mxBean.getName} ${mxBean.getVersion}",
      available_processors = mxBean.getAvailableProcessors
    )
  }

  private val runtime = Eval.always {
    val mxBean = ManagementFactory.getRuntimeMXBean
    RuntimeMX(
      pid = mxBean.getPid,
      virtual_machine = s"${mxBean.getVmVendor} ${mxBean.getVmName} ${mxBean.getVmVersion}",
      input_arguments = mxBean.getInputArguments.asScala.toList
    )
  }

  val allJvmGauge: Eval[AllJvmGauge] =
    for {
      rt <- runtime
      cl <- classloader
      dl <- deadlocks
      gc <- garbage_collectors
      hp <- heap_memory
      nh <- non_heap_memory
      ts <- thread_state
      os <- operating_system
    } yield AllJvmGauge(
      operating_system = os,
      runtime = rt,
      classloader = cl,
      deadlocks = dl,
      garbage_collectors = gc,
      heap_memory = hp,
      non_heap_memory = nh,
      thread_state = ts
    )
}
