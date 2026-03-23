package mccct
package test

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import java.util.concurrent.ConcurrentHashMap
import Scheduler.schedulePoint

import gears.async.Async
import gears.async.default.given
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.concurrent.CyclicBarrier

//@RunWith(classOf[JUnit4])
class DeadlockTest() {

  // @Test
  def deadlockTestWithParallelCode(): Unit = {
    val counter          = AtomicInteger(0)
    def testFunc(): Unit = {
      val testLock1: Lock = new ReentrantLock
      val testLock2: Lock = new ReentrantLock
      val barr            = new CyclicBarrier(3)

      Async.blocking:
        val v1 = Future {
          schedulePoint(1, true, false)
          testLock1.lockInterruptibly()
          try
            barr.await()
            testLock2.lockInterruptibly()
            try
              ()
            finally
              testLock2.unlock()
          finally testLock1.unlock()
          schedulePoint()
        }
        val v2 = Future {
          schedulePoint(2, true, false)
          testLock2.lockInterruptibly()
          try
            barr.await()
            testLock1.lockInterruptibly()
            try
              ()
            finally
              testLock1.unlock()
          finally testLock2.unlock()
          schedulePoint()
        }
        val v3 = Future {
          barr.await()
          Thread.sleep(3500)
          counter.incrementAndGet() // Should not be reached since timeout will happen before
        }
        v3.await // Will cause main thread to panic
        val v4 = Future {
          counter.incrementAndGet()
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(Scheduler.getNumErrors() > 0)
        assert(true)
        assert(counter.get() == 0)
      case _ => assert(false)
  }

  // @Test
  def deadlockTestWithCodeAfter(): Unit = {
    val counter          = AtomicInteger(0)
    def testFunc(): Unit = {
      val testLock1: Lock = new ReentrantLock
      val testLock2: Lock = new ReentrantLock
      val barr            = new CyclicBarrier(2)

      Async.blocking:
        val v1 = Future {
          schedulePoint(1, true, false)
          testLock1.lockInterruptibly()
          try
            barr.await()
            testLock2.lockInterruptibly()
            try
              ()
            finally
              testLock2.unlock()
          finally testLock1.unlock()
          schedulePoint()
        }
        val v2 = Future {
          schedulePoint(2, true, false)
          testLock2.lockInterruptibly()
          try
            barr.await()
            testLock1.lockInterruptibly()
            try
              ()
            finally
              testLock1.unlock()
          finally testLock2.unlock()
          schedulePoint()
        }
        val v3 = Future {
          barr.await()
        }
        v3.await
        Thread.sleep(3500) // Wait until timeout
        val v4 = Future {
          counter.incrementAndGet() // Should not be executed
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(Scheduler.getNumErrors() > 0)
        assert(true)
        assert(counter.get() == 0)
      case _ => assert(false)
  }

  // @Test
  def deadlockTestGeneral(): Unit = {
    def testFunc(): Unit = {
      val testLock1 = new ReentrantLock
      val testLock2 = new ReentrantLock
      val barr      = new CyclicBarrier(2)
      Async.blocking:
        val v1 = Future {
          schedulePoint(1, true, false)
          testLock1.lock()
          try
            barr.await()
            barr.reset()
            testLock2.lockInterruptibly()
            try
              () // Unreachable
            finally
              testLock2.unlock()
          finally testLock1.unlock()
          schedulePoint()
        }
        val v2 = Future {
          schedulePoint(2, false)
          testLock2.lock()
          try
            barr.await()
            testLock1.lockInterruptibly()
            try
              ()
            finally
              testLock1.unlock()
          finally testLock2.unlock()
          schedulePoint()
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(Scheduler.getNumErrors() > 0)
        assert(true)
      case _ => assert(false)
  }

  // @Test
  def deadlockTestNested(): Unit = {
    def testFunc(): Unit = {
      val testLock1 = new ReentrantLock
      val testLock2 = new ReentrantLock
      val barr      = new CyclicBarrier(2)
      Async.blocking:
        Future {
          Future {
            Future {
              val v1 = Future {
                schedulePoint(1, true, false)
                testLock1.lock()
                try
                  barr.await()
                  barr.reset()
                  testLock2.lockInterruptibly()
                  try
                    () // Unreachable
                  finally
                    testLock2.unlock()
                finally testLock1.unlock()
                schedulePoint()
              }
              val v2 = Future {
                schedulePoint(2, false)
                testLock2.lock()
                try
                  barr.await()
                  testLock1.lockInterruptibly()
                  try
                    () // Unreachable
                  finally
                    testLock1.unlock()
                finally testLock2.unlock()
                schedulePoint()
              }
            }
          }
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(Scheduler.getNumErrors() > 0)
      case e => assert(false)
  }

  // @Test
  def deadlockTestWithNoAwait(): Unit = {
    def testFunc(): Unit = {
      Async.blocking:
        val v1 = Future {
          schedulePoint(1, true, false)
          Thread.sleep(5000)
          schedulePoint()
        }
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(Scheduler.getNumErrors() > 0)
        assert(true)
      case _ => assert(false)
  }

  // @Test
  def deadlockRecognitionTestWithAwait(): Unit = {
    def testFunc(): Unit = {
      Async.blocking:
        val v1 = Future {
          schedulePoint(1, true, false)
          Thread.sleep(5000)
          schedulePoint()
        }
        v1.await
    }
    try
      Scheduler.start(RandomWalk, shouldPrint = false)
      testFunc()
      Scheduler.awaitTermination()
      assert(false)
    catch
      case e: DeadlockException =>
        assert(true)
        assert(Scheduler.getNumErrors() > 0)
      case _ => assert(false)
  }
}
