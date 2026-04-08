package mccct

import gears.async.Async
import gears.async.default.given
import java.util.concurrent.atomic.AtomicReference
import CoverageTracker.marker
import Scheduler.schedulePoint
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import gears.async.Async
import gears.async.default.given

object BadExample {

  val map                                                            = ConcurrentHashMap[Int, Int]()
  def insert(key: Int, value: Int)(using Async, Controller): Boolean = {
    schedulePoint()
    if (!map.containsKey(key))
      schedulePoint()
      map.put(key, value)
      schedulePoint()
      true
    else
      schedulePoint()
      schedulePoint()
      false
  }

  def insertTwo(): (Boolean, Boolean) = {
    var v1 = false
    var v2 = false
    Async.blocking:
      val t1 = Future { insert(1, 0) }
      val t2 = Future { insert(1, 1) }

      v1 = t1.await
      v2 = t2.await

    if (v1 == v2) then marker("1.")

    return (v1, v2)
  }

  @main
  def runPosterSessionEx(): Unit =
    CoverageTracker.reset()
    map.clear()
    val (coverageReport, failedSchedules) = CoverageTracker.trackCoverage(
      insertTwo(),
      RandomWalk,
      List("1."),
      true,
      FiniteDuration(5, SECONDS),
      false
    )
    val schedule = Scheduler.getSchedule()
    println(s"schedule was: ${schedule}")
    println(s"Coveragereport was: ${coverageReport}")
    println(s"failedSchedules was: ${failedSchedules}")
    Scheduler.writeSchedule("posterSession.sch")

  @main
  def replayPosterSessionEx(): Unit =
    map.clear()
    val inSchedule = Scheduler.readSchedule("posterSession.sch")
    Scheduler.start(FixedSchedule(inSchedule), shouldPrint = false, sequential = true)
    val (v1, v2) = insertTwo()
    Scheduler.awaitTermination()

    val schedule = Scheduler.getSchedule()
    println(s"schedule was: ${schedule}")
    println(s"Output was: (${v1}, ${v2})")

  @main
  def replayPosterSessionUpdated(): Unit =
    map.clear()
    val inSchedule = Scheduler.readSchedule("posterSession.sch")
    Scheduler.start(FixedSchedule(inSchedule), shouldPrint = false, sequential = true)
    val (v1, v2) = insertTwoUpdated()
    Scheduler.awaitTermination()

    val schedule = Scheduler.getSchedule()
    println(s"schedule was: ${schedule}")
    println(s"Output was: (${v1}, ${v2})")

  def insertTwoUpdated(): (Boolean, Boolean) = {
    var v1 = false
    var v2 = false
    Async.blocking:
      val t1 = Future { insertUpdated(1, 0) }
      val t2 = Future { insertUpdated(1, 1) }

      v1 = t1.await
      v2 = t2.await

    if (v1 == v2) then marker("1.")

    return (v1, v2)
  }

  val lock                                                                  = SchedulerLock()
  def insertUpdated(key: Int, value: Int)(using Async, Controller): Boolean = {
    schedulePoint()
    lock.lock()
    try
      if (!map.containsKey(key))
        schedulePoint()
        map.put(key, value)
        schedulePoint()
        true
      else
        schedulePoint()
        false
    finally
      lock.unlock()
  }
}
