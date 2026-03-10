package mccct

import java.util.concurrent.CyclicBarrier
import gears.async.Cancellable
import java.util.concurrent.atomic.AtomicInteger

enum ControllerType:
  case Async, Finish, Base, Actor

object Controller {

  given rootController: Controller = Controller(null)

}

class Controller(
    val parent: Controller,
    val isEnd: Boolean = false,
    val controllerType: ControllerType = ControllerType.Base
) {
  val id: Id        = Id(parent, isEnd)
  var globalId: Int = -1

  private var scheduleIndex: Int                                   = -1
  private var possibleFailuresEncountered: Int                     = 0
  private var currentFailureSchedule: Vector[Boolean]              = Vector()
  private var nextFailureSchedule: Vector[Boolean]                 = Vector()
  private var storedFailureSchedules: List[(Int, Vector[Boolean])] = List[(Int, Vector[Boolean])]()

  val totalChildren = new AtomicInteger(0)

  var ready: Boolean = false

  var thread: Thread = null

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

}
