package mccct

import gears.async
import scala.collection.mutable
import gears.async.Async

class Coverage() {
  private var expectedPoints: Iterable[Int] = List.empty[Int]
  private val coveredPoints                 = mutable.Map.empty[Int, Int]

  def hasExpectedPoints(): Boolean =
    expectedPoints.nonEmpty

  def setExpectedPoints(expectedPoints: Iterable[Int]): Unit =
    this.expectedPoints = expectedPoints

  def cover(id: Int): Unit =
    coveredPoints.updateWith(id):
      case Some(n) => Some(n + 1)
      case None    => Some(1)

  def count(id: Int): Int =
    coveredPoints.getOrElse(id, 0)

  def allCovered(): Boolean =
    expectedPoints.forall(coveredPoints.contains)

  def pointsToCoverCount(): Int =
    expectedPoints.size

  def pointsCoveredCount(): Int =
    coveredPoints.size
}

object Benchmark {
  var iterations         = 0
  var errors             = 0
  var uncaughtExceptions = 0
  var failedAssertions   = 0
  var pointsToCoverCount = 0
  var pointsCoveredCount = 0

  def reset(): Unit = {
    iterations = 0
    errors = 0
    uncaughtExceptions = 0
    failedAssertions = 0
    pointsToCoverCount = 0
    pointsCoveredCount = 0
  }

  def runUntilCovered[T](
      benchmark: (gears.async.Async, Controller, Coverage) ?=> T,
      maxIterations: Integer,
      method: RunMethod,
      assertion: T => Boolean = (_: T) => true, // Default: no assertion,
      afterIteration: () => T = () => (),
      printSchedules: Boolean = false,
      printSummary: Boolean = true
  ): Unit = {
    // Reset in case benchmark has been run previously
    Benchmark.reset()
    // The benchmark needs a coverage instance available
    given tracker: Coverage = new Coverage
    var running             = true
    while (running) {
      iterations += 1
      // Run a McCCT iteration of the benchmark using the supplied method
      val (res, canContinue) = method.runIteration(benchmark)
      // Update coverage information
      if iterations == 1 then pointsToCoverCount = tracker.pointsToCoverCount()
      pointsCoveredCount = tracker.pointsCoveredCount()
      // Check if the iteration is correct or not
      Benchmark.isErroneousRun(res, assertion)
      // Run a method after each iteration
      afterIteration()
      // Check if all expected markers in the benchmark has been hit
      val allCovered = tracker.allCovered()
      // If the method cannot continue, every marker has been hit, or we have reached the iteration bound: exit
      if !canContinue || allCovered || iterations >= maxIterations then running = false
      if printSchedules then println(f"[Iteration $iterations%04d]: ${Scheduler.scheduleToString()}")
    }

    if printSummary then Benchmark.printSummary()
  }

  def printSummary(): Unit = {
    println(f"""
    ===== Benchmark Summary =====
    Iterations:          $iterations
    -----------------------------
    Uncaught exceptions: $uncaughtExceptions
    Failed assertions:   $failedAssertions
    Total errors:        $errors
    Error rate:          ${errors.toDouble / iterations}%.3f
    -----------------------------
    Points covered:      $pointsCoveredCount
    Points to cover:     $pointsToCoverCount
    Cover rate:          ${pointsCoveredCount.toDouble / pointsToCoverCount}%.3f
    =============================
    """)
  }

  def coveragePoint(id: Int)(using tracker: Coverage): Unit = {
    tracker.cover(id)
  }

  def setCoveragePointCount(count: Int)(using tracker: Coverage): Unit = {
    if !tracker.hasExpectedPoints() then tracker.setExpectedPoints((1 to count))
  }

  def isErroneousRun[T](res: Option[T], assertion: T => Boolean): Unit = {
    if Scheduler.getNumErrors() > 0 then
      uncaughtExceptions += 1
      errors += 1
      return
    res match {
      case Some(value) =>
        if !assertion(value) then
          failedAssertions += 1
          errors += 1
      case None => errors += 1 // Should not happen, since None should be due to an uncaught exception
    }
  }
}
