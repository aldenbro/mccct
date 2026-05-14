import mccct._
import mccct.Scheduler.possibleFailureWithId
import mccct.Benchmark.coveragePoint
import mccct.Benchmark.setCoveragePointCount

/** Returns a boolean representing if a scheduling event will occur. The probability of an event occurring will be the
  * input fraction given that the scheduler is scheduling in a random fashion.
  *
  * @param numerator
  * @param denominator
  * @return
  */
def generateSchedulingEvent(numerator: Int, denominator: Int)(using gears.async.Async, Controller): Boolean = {
  val lock         = SchedulerLock()
  val condition    = lock.newCondition()
  var triggerEvent = false

  lock.lock()

  (1 to numerator).foreach(_ =>
    Future {
      lock.lock()
      triggerEvent = true
      condition.signal()
      lock.unlock()
    }
  )

  (1 to (denominator - numerator)).foreach(_ =>
    Future {
      lock.lock()
      triggerEvent = false
      condition.signal()
      lock.unlock()
    }
  )

  condition.await()
  val res = triggerEvent
  lock.unlock()

  res
}

/** A benchmark where the hidden coverage point can be reached by either a coverage event or by failure injection.
  *
  * @param numberOfFutures
  *   number of possible events
  * @param numberOfCAFChecks
  *   number of events where the non-injected data path is checked for coverage
  * @param schedulingEventProbabilityNumerator
  *   part of the rational that forms the probability of a scheduling event occurring
  * @param schedulingEventProbabilityDenominator
  *   part of the rational that forms the probability of a scheduling event occurring
  */
def failureOrConcurrency(
    numberOfFutures: Int,
    numberOfCAFChecks: Int,
    schedulingEventProbabilityNumerator: Int,
    schedulingEventProbabilityDenominator: Int
)(using gears.async.Async, Controller, Coverage) = {
  setCoveragePointCount(numberOfFutures + numberOfCAFChecks)

  (1 to numberOfFutures).foreach(i =>
    Future {
      try
        // Scheduling event leads to a failure, or we inject it
        if generateSchedulingEvent(schedulingEventProbabilityNumerator, schedulingEventProbabilityDenominator) then
          throw new RuntimeException
        possibleFailureWithId(i, new RuntimeException)
        // We perform some additional checks for a failure not being triggered.
        // Otherwise, the optimal strategy would be to always inject a failure,
        // which is not realistic.
        if i <= numberOfCAFChecks then coveragePoint(i + numberOfFutures)
      catch
        case _ =>
          coveragePoint(i)
    }
  )
}

/** A benchmark where coverage events are run after an injected failure.
  *
  * @param numberOfFutures
  *   number of possible events
  * @param numberOfCAFChecks
  *   number of events where the non-injected data path is checked for coverage
  * @param schedulingEventProbabilityNumerator
  *   part of the rational that forms the probability of a scheduling event occurring
  * @param schedulingEventProbabilityDenominator
  *   part of the rational that forms the probability of a scheduling event occurring
  */
def concurrencyAfterFailure(
    numberOfFutures: Int,
    numberOfCAFChecks: Int,
    schedulingEventProbabilityNumerator: Int,
    schedulingEventProbabilityDenominator: Int
)(using gears.async.Async, Controller, Coverage) = {
  setCoveragePointCount(numberOfFutures + numberOfCAFChecks)

  (1 to numberOfFutures).foreach(i =>
    Future {
      try
        possibleFailureWithId(i, new RuntimeException)
        // We perform some additional checks for a failure not being triggered.
        // Otherwise, the optimal strategy would be to always inject a failure,
        // which is not realistic.
        if i <= numberOfCAFChecks then coveragePoint(i + numberOfFutures)
      catch
        case _ =>
          if generateSchedulingEvent(schedulingEventProbabilityNumerator, schedulingEventProbabilityDenominator) then
            coveragePoint(i)
    }
  )
}

/** A benchmark where failure injection points are reached after a specific scheduling occurs.
  *
  * @param numberOfFutures
  *   number of possible events
  * @param numberOfCAFChecks
  *   number of events where the non-injected data path is checked for coverage
  * @param schedulingEventProbabilityNumerator
  *   part of the rational that forms the probability of a scheduling event occurring
  * @param schedulingEventProbabilityDenominator
  *   part of the rational that forms the probability of a scheduling event occurring
  */
def failureAfterConcurrency(
    numberOfFutures: Int,
    numberOfCAFChecks: Int,
    schedulingEventProbabilityNumerator: Int,
    schedulingEventProbabilityDenominator: Int
)(using gears.async.Async, Controller, Coverage): Unit = {
  setCoveragePointCount(numberOfFutures + numberOfCAFChecks)

  (1 to numberOfFutures).foreach(i =>
    Future {
      // We first see if a scheduling event occurs
      if generateSchedulingEvent(schedulingEventProbabilityNumerator, schedulingEventProbabilityDenominator) then
        try
          // Then we possibly inject a failure
          possibleFailureWithId(i, new RuntimeException)
          // We perform some additional checks for a failure not being triggered.
          // Otherwise, the optimal strategy would be to always inject a failure,
          // which is not realistic.
          if i <= numberOfCAFChecks then coveragePoint(i + numberOfFutures)
        catch
          case _ =>
            // We check that we can reach points dependent on injection after a scheduling event
            coveragePoint(i)
    }
  )
}
