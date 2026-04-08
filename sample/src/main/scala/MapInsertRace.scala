package mccct.sample

import java.util.concurrent.ConcurrentHashMap

import gears.async.Async
import gears.async.default.given

import mccct.{Scheduler, Future, FixedSchedule, Controller}
import mccct.Scheduler.schedulePoint

object MapInsertRace {

  /** Due to a race, this method can return (true, true).
    */
  def concurrentInsert(): (Boolean, Boolean) = {
    val map = ConcurrentHashMap[Int, Int]()

    def insert(key: Int, value: Int)(using a: Async, parent: Controller): Boolean = {
      if !map.containsKey(key) then
        schedulePoint()
        map.put(key, value)
        schedulePoint()
        true
      else
        schedulePoint()
        schedulePoint()
        false
    }

    val (v1, v2) = Async.blocking:
      val f1 = Future { insert(1, 0) }
      val f2 = Future { insert(1, 1) }
      (f1.await, f2.await)
    (v1, v2)
  }

  @main
  def run(): Unit =
    println("MapInsertRace running...")

    // schedule that reproduces the race
    val s = List("1.", "2.", "1.", "2.", "2.", "2.0.", "1.", "1.0.", "", "")

    Scheduler.start(FixedSchedule(s), shouldPrint = false, sequential = true)
    val res = concurrentInsert()
    Scheduler.awaitTermination()
    val schedule = Scheduler.getSchedule()
    assert(res == (true, true))

    println("Checking reliability...")
    // check that race is reproduced reliably
    assert(Scheduler.checkReliability(concurrentInsert(), (true, true), schedule, 20, 1.0, true))

}
