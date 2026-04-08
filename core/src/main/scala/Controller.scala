package mccct

import java.util.concurrent.CyclicBarrier
import gears.async.Cancellable
import java.util.concurrent.atomic.AtomicInteger

// ? Async type is never used
enum ControllerType:
  case Async, Finish, Base, Actor

object Controller {
  // ! Not needed with new method
  given rootController: Controller = Controller(null)
}

class Controller(
    val parent: Controller,
    val isEnd: Boolean = false,
    val controllerType: ControllerType = ControllerType.Base
) {

  // Associated tasks includes children, end tasks, and possibly itself.
  // It also contains a boolean whether or not the task should be counted when submitted.
  @volatile
  private var associatedTasks: List[(Controller, Boolean)] = List[(Controller, Boolean)]()

  val id: Id        = Id(parent, isEnd)
  var globalId: Int = -1

  private[mccct] var scheduleIndex: Int                                   = -1
  private[mccct] var possibleFailuresEncountered: Int                     = 0
  private var currentFailureSchedule: Vector[Boolean]              = Vector()
  private var nextFailureSchedule: Vector[Boolean]                 = Vector()
  private var storedFailureSchedules: List[(Int, Vector[Boolean])] = List[(Int, Vector[Boolean])]()

  val totalChildren = new AtomicInteger(0)

  var ready: Boolean = false

  var thread: Thread = null

  @volatile var heldLocks: List[SchedulerLock]     = List()
  @volatile var waitingLock: Option[SchedulerLock] = None

  var timeoutTask: Option[Cancellable] = None

  def addTimeoutTask(timeout: Option[Cancellable]): Unit =
    timeoutTask.foreach(_.cancel())
    timeoutTask = timeout

  private val schedulerBarrier = new CyclicBarrier(2)

  def await(index: Int = -1) =
    // The scheduler will supply an index when the task is ready to run,
    // this index is later used to append the failure injection information
    // to the schedule.
    if index >= 0 then
      pushCurrentFailures()
      scheduleIndex = index
    schedulerBarrier.await()

  def reset() = schedulerBarrier.reset()

  def waitForLock(lock: SchedulerLock) =
    waitingLock = Some(lock)

  def acquireLock(lock: SchedulerLock) =
    heldLocks = lock +: heldLocks
    waitingLock = None

  def releaseLock(lock: SchedulerLock) =
    heldLocks = heldLocks diff List(lock)

  def isWaiting = waitingLock.isDefined

  def resetWaiting = waitingLock = None

  def waitingFor = waitingLock.get

  def holdsLock(lock: SchedulerLock) = heldLocks.contains(lock)

  private val conditionBarrier = new CyclicBarrier(2)

  def awaitCondition() =
    if !conditionBarrier.isBroken then conditionBarrier.await()

  def resetCondition() = conditionBarrier.reset()

  final def isRoot = parent == null // Signifies if it is root, which means that it controls the main thread

  override def toString(): String = s"[${id.getId()}]"

  override def equals(x: Any): Boolean = x match
    case ctrl: Controller => ctrl.id.getId() == this.id.getId()
    case _                => false

  private[mccct] def closestType(parentType: ControllerType): Controller =
    this.controllerType match
      case `parentType`     => return this
      case _ if this.isRoot => return this
      case _                => return parent.closestType(parentType)

  private[mccct] def startThread(task: Runnable, maxId: Int): Thread =
    globalId = maxId
    Thread.ofVirtual().start(task)

  private[mccct] def setNextFailureSchedule(schedule: Vector[Boolean]): Unit =
    // So that fixed schedule exploration can tell the controller what choices to make
    nextFailureSchedule = schedule

  private[mccct] def getNextFailureSchedule(): Vector[Boolean] =
    val schedule = nextFailureSchedule
    nextFailureSchedule = Vector()
    schedule

  private[mccct] def hasScheduledChoice(): Option[Boolean] =
    val result =
      if currentFailureSchedule.length > possibleFailuresEncountered then
        Some(currentFailureSchedule(possibleFailuresEncountered))
      else None
    // Calling this function means we encountered an injection point
    possibleFailuresEncountered += 1
    result

  private[mccct] def appendInjectionChoice(choice: Boolean): Unit =
    // Keeps track of the choices made by the failure exploration algorithm
    currentFailureSchedule = currentFailureSchedule :+ choice

  private[mccct] def pushCurrentFailures(): Unit =
    // When a controller is reused/finished we push and clear the failure points encountered
    if currentFailureSchedule.nonEmpty then
      storedFailureSchedules = (scheduleIndex, currentFailureSchedule) :: storedFailureSchedules
    currentFailureSchedule = getNextFailureSchedule()
    possibleFailuresEncountered = 0

  private[mccct] def getFailures(): List[(Int, Vector[Boolean])] =
    pushCurrentFailures()
    storedFailureSchedules

  private[mccct] def addAssociatedTask(controller: Controller, shouldIncrement: Boolean = true): Unit =
    associatedTasks = (controller, shouldIncrement) :: associatedTasks

  private[mccct] def getAndClearAssociatedTasks(): List[(Controller, Boolean)] =
    val res = associatedTasks
    associatedTasks = List[(Controller, Boolean)]()
    res
}
