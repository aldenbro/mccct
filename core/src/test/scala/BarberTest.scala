package mccct
package test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import CoverageTracker.marker
import Scheduler.schedulePoint

import gears.async
import gears.async.Async
import gears.async.default.given
import gears.async.Scheduler
import scala.concurrent.duration.{FiniteDuration, SECONDS}

import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.io.File

@RunWith(classOf[JUnit4])
class BarberTest {

  val done = AtomicBoolean(false)
  val rnd  = new scala.util.Random

  class Barber {

    var freeSeats = 2

    var waitingQueue = List[Int]()

    val lock            = SchedulerLock()
    val barberReady     = lock.newCondition()
    val customerReady   = lock.newCondition()
    val terminationLock = lock.newCondition()

    var barberIsSleeping = true
    var servedCustomer   = 0

    def checkTermination()(using scheduler: Scheduler, ac: async.Async, controller: Controller): Unit = {
      scheduler.schedule(
        FiniteDuration(4, SECONDS),
        new Runnable {
          def run() =
            lock.lock(true)
            try
              done.set(true)
              customerReady.signal()
              barberReady.signalAll()
            finally lock.unlock()
        }
      )
    }

    def barber()(using async.Async, Controller): Unit =
      while (!done.get()) { // Run until timeout
        schedulePoint()
        lock.lock()
        try
          if (waitingQueue.isEmpty) then // If we do not have someone in queue wait until there is someone
            barberIsSleeping = true      // The problem is here that the await does not make us change schedule
            customerReady.await()        // Wait until a customer (or timeout) signals
            if (done.get()) then
              marker("3.")
              lock.unlock()
              return
            marker("4.")
            // assert(!waitingQueue.isEmpty) // Assert that the queue is non-empty

          marker("5.")
          val customerId =
            waitingQueue.head // Take out the customerId to cut (is not a guarantee in this case but seems to work)
          waitingQueue = waitingQueue.tail
          barberIsSleeping = false
          barberReady.signal() // Signal a customer that it can be cut
          servedCustomer += 1
        finally
          lock.unlock()
          schedulePoint()

        Thread.sleep(rnd.nextLong(25)) // Simulate the hair cutting
      }

    def okcustomer(id: Int)(using async.Async, Controller): Unit =
      lock.lock()
      try
        if freeSeats > waitingQueue.size then      // If there is space then go into the shop
          assert(!waitingQueue.contains(id))       // Assert that customer is not in shop
          waitingQueue = waitingQueue ::: List(id) // Add customer to queue
          // println(s"Customer ${id}: added to the queue")

          customerReady.signal() // Signal that there is a ready customer
          barberReady.await()    // Wait until barber has time
        // println(s"Customer ${id}: Is getting a haircut.")
        else ()
      finally
        lock.unlock()

    def badcustomer(id: Int)(using async.Async, Controller): Unit =
      schedulePoint()
      if waitingQueue.size < freeSeats && !done.get() then
        schedulePoint()
        lock.lock()
        try
          if done.get() then
            lock.unlock()
            return
          if waitingQueue.size >= freeSeats then
            marker("1.")
            lock.unlock()
            assert(false)
          marker("2.")
          waitingQueue = waitingQueue ::: List(id)

          customerReady.signal()
          barberReady.await()
        finally
          lock.unlock()
          schedulePoint()
      else schedulePoint()
  }

  @Test
  def barberShop(): Unit = {
    println("Starting barber shop test")
    CoverageTracker.reset()
    def barberFunc(): Unit = {
      val numCustomers = 6
      done.set(false)
      val barb = new Barber
      Async.blocking:
        val f1 = Future {
          Future {
            barb.barber()
          }
          val v1 = Future {
            barb.checkTermination()
          }
          v1.await
        }
        f1.await
        val futures = for (id <- 1 to numCustomers) yield Future {
          Thread.sleep(rnd.nextLong(15))
          barb.badcustomer(id)
        }
    }
    val (coverageReport, failedRuns) =
      CoverageTracker.trackCoverage(
        barberFunc(),
        RandomWalk,
        List("1.", "2.", "3.", "4.", "5."),
        sequential = true,
        delay = FiniteDuration(45, SECONDS),
        shouldReport = false
      )
    val schedules = failedRuns.keySet.toList
    assert(!schedules.isEmpty)
    val schedule1 = schedules(0)
    println("Schedule is: " + schedule1)
    assert(schedule1.size > 0)
    val outputFile                        = "test-trace.txt"
    val (hasNoErrors, hasReliableResults) =
      Scheduler.handleErrors(
        barberFunc(),
        (),
        schedule1,
        outputFile = outputFile,
        sequential = true,
        iters = 10,
        debug = false
      )
    try
      assert(!hasNoErrors)
      assert(hasReliableResults)
      assert(CoverageTracker.missedMarkers.size == 0)
    finally
      val f = new File(outputFile)
      f.delete()
  }
}
