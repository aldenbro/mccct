package mccct
package test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import mccct.CoverageTracker.marker
import mccct.Scheduler.schedulePoint

import gears.async
import gears.async.Async
import gears.async.default.given
import gears.async.Scheduler
import scala.concurrent.duration.{FiniteDuration, SECONDS}

import java.util.concurrent.locks.{Lock, ReentrantLock, Condition}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.CyclicBarrier
import java.io.File

//@RunWith(classOf[JUnit4])
class SmokerTest {

  val done = AtomicBoolean(false)
  val rnd  = new scala.util.Random

  val barrier = new CyclicBarrier(2)

  class Smoker() {

    val lock    = new SchedulerLock(new ReentrantLock)
    val paper   = lock.newCondition()
    val matches = lock.newCondition()
    val tobacco = lock.newCondition()

    val smokerDone = lock.newCondition()

    def checkTermination()(using scheduler: Scheduler, ac: async.Async, task: Controller): Unit = {
      scheduler.schedule(
        FiniteDuration(10, SECONDS),
        new Runnable {
          def run() =
            lock.lock()
            try
              println("Timeout things")
              paper.signalAll()
              matches.signalAll()
              tobacco.signalAll()
              smokerDone.signalAll()
              done.set(true)
            finally lock.unlock()
        }
      )
    }

    def nonSmokerAgent()(using ac: async.Async, controller: Controller): Unit = {
      // println("Starting nonsmokerAgent")
      barrier.await()
      while (!done.get()) {
        schedulePoint(10, true)
        lock.lock()
        schedulePoint()
        try
          // Choose two random ingredients to add to the table
          val ing = rnd.between(0, 2)
          ing match
            case 0 =>
              paper.signal()
              tobacco.signal()
            case 1 =>
              tobacco.signal()
              matches.signal()
            case 2 =>
              matches.signal()
              paper.signal()
          // Wait for doneSmoker
          smokerDone.await()
          println("Done with iteration")
        finally lock.unlock()
      }
    }

    def tobaccoSmoker()(using ac: async.Async, controller: Controller): Unit = {
      // println(s"Starting real thing tobacco (${task})")
      while (!done.get()) {
        schedulePoint(7, true, false)
        lock.lock()
        try
          // println("tobaccoSmoker is waiting for matches")
          schedulePoint(8, true, false)
          matches.await()
          schedulePoint()
          if done.get() then return
          // println("tobaccoSmoker is waiting for paper")
          schedulePoint(9, true, false)
          paper.await()
          schedulePoint()
          if done.get() then return
          // println("tobaccoSmoker is smoking")
          smoke()
          smokerDone.signal()
        finally lock.unlock()
      }
    }

    def matchesSmoker()(using ac: async.Async, controller: Controller): Unit = {
      // println(s"Starting real thing matches (${task})")
      while (!done.get()) {
        schedulePoint(4, true, false)
        lock.lock()
        try
          // println("matchesSmoker is waiting for paper")
          schedulePoint(5, true, false)
          paper.await()
          schedulePoint()
          if done.get() then return
          // println("matchesSmoker is waiting for tobacco")
          schedulePoint(6, true, false)
          tobacco.await()
          schedulePoint()
          if done.get() then return
          // println("matchesSmoker is smoking")
          smoke()
          smokerDone.signal()
        finally lock.unlock()
      }
    }

    def paperSmoker()(using ac: async.Async, controller: Controller): Unit = {
      // println(s"Starting real thing paper (${task})")
      while (!done.get()) {
        schedulePoint(1, true, false)
        lock.lock()
        try
          // println("paperSmoker is waiting for tobacco")
          schedulePoint(2, true, false)
          tobacco.await()
          schedulePoint()
          if done.get() then return
          // println("paperSmoker is waiting for matches")
          schedulePoint(3, true, false)
          matches.await()
          schedulePoint()
          if done.get() then return
          // println("paperSmoker is smoking")
          smoke()
          smokerDone.signal()
        finally lock.unlock()
      }
    }

    def smoke(): Unit = {
      Thread.sleep(rnd.nextLong(500))
    }

  }

  // TODO: Fix deadlock recognition
  // @Test
  def smokerTest(): Unit = {
    println("Starting smokerTest")
    CoverageTracker.reset()
    def smokerFunc(): Unit = {
      done.set(false)
      val smokeRoom = new Smoker()
      Async.blocking:
        val f1 = Future {
          Future {
            smokeRoom.nonSmokerAgent()
          }
          val v1 = Future {
            smokeRoom.checkTermination()
          }
          barrier.await()
        }
        f1.await
        Future {
          smokeRoom.tobaccoSmoker()
        }
        Future {
          smokeRoom.matchesSmoker()
        }
        Future {
          smokeRoom.paperSmoker()
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      smokerFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch case e: DeadlockException => ()
  }
}
