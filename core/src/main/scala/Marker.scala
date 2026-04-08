package mccct

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{FiniteDuration, SECONDS}

import gears.async.{Cancellable, Scheduler}
import gears.async.default.given

object CoverageTracker {
  // Map that saves how many times each marker has been hit
  private val markerMap = TrieMap[String, Int]()
  // Map that saves what schedule led to a specific list of markers (since each marker is either hit or unhit in one execution of the program)
  private var scheduleMarkerList = List[(List[String], List[String])]()
  // List that stores which markers the user expects the tracker to hit
  private var expectedMarkers = List[String]()

  private val done                          = AtomicBoolean(false)
  private var stopTask: Option[Cancellable] = None
  private var timeoutDelay                  = FiniteDuration(10, SECONDS)

  private def resetStopTask() = this.synchronized {
    // Stops any running tasks
    stopTask.foreach(_.cancel())
    // Set the currently running task to none
    stopTask = None
  }

  // Since each future can call marker concurrently it should be necessary with some kind of lock
  private def stopExecution(delay: FiniteDuration)(using scheduler: Scheduler) = this.synchronized {
    // First if there is a currently running stopTask that task must be canceled
    resetStopTask()
    // Then start a new task with the scheduler that will change the done flag after the delay
    stopTask = Some(
      scheduler.schedule(
        delay,
        new Runnable {
          def run() =
            // Set `done` to true, allowing the loop in `trackCoverage` to finish
            done.set(true)
        }
      )
    )
  }

  def marker(id: String) = markerMap.updateWith(id) {
    // If there was a previous value then add 1 to it
    case Some(value) =>
      Some(value + 1)
    // If there was no previous value set it to 1, since it has now been hit once
    case None => {
      stopExecution(timeoutDelay)
      Some(1)
    }
  }

  def updateExpectedMarkers(markers: List[String]) = this.synchronized {
    expectedMarkers = markers
  }

  def missedMarkers: List[String] = this.synchronized {
    expectedMarkers diff markerMap.keySet.toList
  }

  def numMarkers: Int = markerMap.values.sum

  def reset() = this.synchronized {
    markerMap.clear()
    scheduleMarkerList = List()
    expectedMarkers = List[String]()
    resetStopTask()
    done.set(false)
  }
  // TODO: change this to a list
  private def addSchedule(map: TrieMap[String, Int]): Unit =
    val schedule = Scheduler.getSchedule()
    scheduleMarkerList = (schedule, map.keySet.toList) +: scheduleMarkerList

  private def diff(map1: TrieMap[String, Int], map2: TrieMap[String, Int]): TrieMap[String, Int] =
    val keys   = map1.keySet ++ map2.keySet
    val result = TrieMap[String, Int]()

    for (key <- keys) {
      val v1   = map1.getOrElse(key, 0)
      val v2   = map2.getOrElse(key, 0)
      val diff = v1 - v2
      if diff != 0 then result(key) = diff
    }
    result

  private def saveAndQuit(shouldReport: Boolean) = {
    // If an error was encountered then that interleaving will be saved
    // If this should be reported to a file, then write each problematic interleaving to its own file
    if !scheduleMarkerList.isEmpty && shouldReport then
      scheduleMarkerList.zipWithIndex.foreach { (content, idx) =>
        val schedule = content._1
        println(s"Writing scheduld ${schedule}")
        Scheduler.writeSchedule(id = (idx + 1).toString(), potentialTarget = Some(schedule.reverse))
      }
    // Since hitting any marker will create the `stopExecution` task
    // it is necessary to reset the task at the end of the function call to leave no dangling tasks
    resetStopTask()
  }

  private def runIteration(
      testFunction: => Unit,
      alg: ExplorationAlgorithm,
      sequential: Boolean,
      prevMap: TrieMap[String, Int]
  ): TrieMap[String, Int] = {
    // Run the function with the given algorithm
    try {
      Scheduler.start(alg, shouldPrint = false, sequential = sequential)
      testFunction
      Scheduler.awaitTermination()
      // If the scheduler has encountered any errors this run, then the getNumErrors will be non-zero
      // If this is the case then we should add which markers were hit (and show that it was an erroneous interleaving)
      // Since only the markers that were hit in this specific interleaving is of note, remove the hit markers from previous interleavings
      if Scheduler.getNumErrors() != 0 then addSchedule(diff(markerMap, prevMap))
      // "Reset" prevMap to an empty map and add the mappings from markerMap
      // Only setting `prevMap = markerMap` will make changes to markerMap affect prevMap
      TrieMap.empty[String, Int] ++ markerMap
    } catch {
      case e: Exception => {
        addSchedule(diff(markerMap, prevMap))
        return TrieMap.empty[String, Int] ++ markerMap
      }
    }
  }

  /** Runs a function a number of times and returns how many times each marker was hit
    *
    * @param testFunction
    *   the function to test
    * @param alg
    *   the exploration algorithm to use
    * @param numIter
    *   the numer of iterations to run the tracker tool for
    * @return
    */
  def trackCoverageIter(
      testFunction: => Unit,
      alg: ExplorationAlgorithm,
      numIter: Int,
      sequential: Boolean = false,
      shouldReport: Boolean = false
  ): (TrieMap[String, Int], TrieMap[List[String], List[String]]) = {
    var counter = 0
    var prevMap = TrieMap[String, Int]()
    while (counter < numIter) {
      prevMap = runIteration(testFunction, alg, sequential, prevMap)
      counter += 1
    }
    saveAndQuit(shouldReport)
    (markerMap, TrieMap.from(scheduleMarkerList))
  }

  def countErrors[T](
      body: => T,
      iterations: Int,
      alg: ExplorationAlgorithm = RandomWalk,
      sequential: Boolean = false
  ): Int =
    val (hitMarkers, failedSchedules) = trackCoverageIter(body, alg, iterations, sequential, true)
    scheduleMarkerList.size

  def trackCoverage(
      testFunction: => Unit,
      alg: ExplorationAlgorithm,
      expected: List[String],
      sequential: Boolean = false,
      delay: FiniteDuration = FiniteDuration(120, SECONDS),
      shouldReport: Boolean = false
  ): (TrieMap[String, Int], TrieMap[List[String], List[String]]) = {
    timeoutDelay = delay
    expectedMarkers = expected
    var hasNoErrors = false
    var prevMap     = TrieMap[String, Int]()
    while (!done.get()) {
      prevMap = runIteration(testFunction, alg, sequential, prevMap)
      hasNoErrors = Scheduler.checkErrors(true)
      // If `missedMarkers` returns an empty list, then all expected markers have been hit
      // In this case we set the `boolean` done to true, which will terminate the while loop..
      if missedMarkers == List() then done.set(true)
    }
    saveAndQuit(shouldReport)
    (markerMap, TrieMap.from(scheduleMarkerList))
  }
}
