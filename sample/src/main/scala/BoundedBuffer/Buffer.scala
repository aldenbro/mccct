package mccct

import scala.reflect.ClassTag

import gears.async
import gears.async.Async
import Scheduler.schedulePoint
import scala.concurrent.duration.{FiniteDuration, SECONDS}

trait Buffer[T] {
  @throws[InterruptedException]
  def put(item: T)(using async.Async, Controller): Unit

  @throws[InterruptedException]
  def get()(using async.Async, Controller): T
}

class BufferImpl[T: ClassTag](val size: Int) extends Buffer[T] {

  val buf   = new Array[T](size)
  var in    = 0
  var out   = 0
  var count = 0

  val lock = SchedulerLock()
  val cond = lock.newCondition()

  @throws[InterruptedException]
  override def put(item: T)(using async.Async, Controller): Unit =
    schedulePoint(0, true, delay = FiniteDuration(5, SECONDS))
    lock.lock()
    try {
      while (count == size) cond.await()

      buf(in) = item
      count += 1
      in = (in + 1) % size

      cond.signal() // Should be `notifyAll()`
      schedulePoint(1)
    } finally {
      lock.unlock()
    }

  @throws[InterruptedException]
  override def get()(using async.Async, Controller): T =
    schedulePoint(2, true, delay = FiniteDuration(5, SECONDS))
    this.synchronized {}
    try {
      while (count == 0) cond.wait()

      val item = buf(out)
      count -= 1
      out = (out + 1) % size

      cond.signal() // Should be `notifyAll()`
      schedulePoint(3)
      item
    } finally {
      lock.unlock()
    }
}
