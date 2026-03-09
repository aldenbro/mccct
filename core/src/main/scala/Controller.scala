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

  var scheduleIndex: Int = -1
  var possibleFailuresEncountered: Int = 0
  var failureSchedule: Vector[Boolean] = Vector()

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
    // this index is used to append information about failure injection
    // when the controller finishes.
    if index >= 0 then scheduleIndex = index
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

}
