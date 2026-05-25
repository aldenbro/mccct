import mccct._
import mccct.Scheduler.possibleFailureWithId
import mccct.Benchmark.coveragePoint
import mccct.Benchmark.setCoveragePointCount
import scala.compiletime.ops.int
import java.util.concurrent.atomic.AtomicReference

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

@main
def testing() = {
  given tracker: Coverage = Coverage()
  Scheduler(alg = RandomWalk, failureAlg = RandomlyInject(0.5)) {
    nestedFailures(5, 1, 1, 0)
  }
}

def nestedFailuresInFutures(
    futuresPerNesting: Int,
    nestingCount: Int,
    dependencies: Int,
    counterdependencies: Int
)(using gears.async.Async, Controller, Coverage) = {
  setCoveragePointCount(futuresPerNesting * nestingCount)

  // We cannot have more dependencies than we have futures available to depend on
  assert(dependencies + counterdependencies < futuresPerNesting)

  val completed = new AtomicReference(Vector.fill(futuresPerNesting * (nestingCount + 1))(false))

  def complete(index: Int) = {
    completed.updateAndGet { vec =>
      vec.updated(index, true)
    }
  }

  // Root level, has no dependencies or counterdependencies
  (0 until futuresPerNesting).foreach(i =>
    Future {
      try possibleFailureWithId(i, new RuntimeException)
      catch
        case _ => {
          // println(f"Coverage for Future $i")
          complete(i)
        }
    }
  )

  for (n <- 1 to nestingCount) {
    val currentOffset  = n * futuresPerNesting
    val previousOffset = (n - 1) * futuresPerNesting
    for (i <- currentOffset until futuresPerNesting + currentOffset) {
      Future {
        // We get the previous level of futures
        val vec   = completed.get().slice(previousOffset, previousOffset + futuresPerNesting)
        val start = i % futuresPerNesting
        // We check that all dependent Futures has been completed
        val dependenciesSatisfied = (0 until dependencies).map(x => vec((start + x) % futuresPerNesting)).forall(b => b)
        // We check that all counterdependent Futures has not been completed
        val counterdependenciesUnsatisfied =
          (0 until counterdependencies).map(x => vec((start + dependencies + x) % futuresPerNesting)).forall(b => !b)
        // If both checks pass we can run the contents of this future
        // println(f"Future $i: d = $dependenciesSatisfied, cd = $counterdependenciesUnsatisfied")
        if dependenciesSatisfied && counterdependenciesUnsatisfied then {
          try possibleFailureWithId(i, new RuntimeException)
          catch
            case _ => {
              // println(f"Coverage for Future $i")
              complete(i)
              coveragePoint(i - futuresPerNesting + 1)
            }
        }
      }
    }
  }
}

def nestedFailures(
    nodes: Int,
    nestingCount: Int,
    dependencies: Int,
    counterdependencies: Int
)(using gears.async.Async, Controller, Coverage) = {
  setCoveragePointCount(nodes * nestingCount + nodes * (nestingCount + 1))

  // We cannot have more dependencies than we have futures available to depend on
  assert(dependencies + counterdependencies < nodes)

  val completed = new AtomicReference(Vector.fill(nodes * (nestingCount + 1))(false))

  def complete(index: Int) = {
    completed.updateAndGet { vec =>
      vec.updated(index, true)
    }
  }

  // Root level, has no dependencies or counterdependencies
  (0 until nodes).foreach(i =>
    try 
      possibleFailureWithId(i, new RuntimeException)
      // CAF check
      coveragePoint(nodes * nestingCount + 1 + i)
    catch
      case _ => {
        // println(f"Coverage for Future $i")
        complete(i)
      }
  )

  for (n <- 1 to nestingCount) {
    val currentOffset  = n * nodes
    val previousOffset = (n - 1) * nodes
    for (i <- currentOffset until nodes + currentOffset) {
      // We get the previous level of futures
      val vec   = completed.get().slice(previousOffset, previousOffset + nodes)
      val start = i % nodes
      // We check that all dependent Futures has been completed
      val dependenciesSatisfied = (0 until dependencies).map(x => vec((start + x) % nodes)).forall(b => b)
      // We check that all counterdependent Futures has not been completed
      val counterdependenciesUnsatisfied =
        (0 until counterdependencies).map(x => vec((start + dependencies + x) % nodes)).forall(b => !b)
      // If both checks pass we can run the contents of this future
      // println(f"Future $i: d = $dependenciesSatisfied, cd = $counterdependenciesUnsatisfied")
      if dependenciesSatisfied && counterdependenciesUnsatisfied then {
        try 
          possibleFailureWithId(i, new RuntimeException)
          // CAF check
          coveragePoint(nodes * (nestingCount + 1) + i - nodes + 1)
        catch
          case _ => {
            // println(f"Coverage for Future $i")
            complete(i)
            coveragePoint(i - nodes + 1)
          }
      }
    }
  }
}
