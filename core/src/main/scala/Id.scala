package mccct

import java.util.concurrent.atomic.AtomicInteger

class Id(val parent: Controller, val isEnd: Boolean = false):

  var numChildren: AtomicInteger = AtomicInteger(0)
  private val id: String         = {
    // The root gets a special id
    if parent == null then "0."
    else
      // The parent id should be prepended if the parent isn't the root,
      // (however the root id is prepended if this is it's end task).
      (if parent.isRoot && !isEnd then "" else parent.id.getId()) +
        // If it's an end task, it gets a special "0." suffix.
        // Otherwise, it gets a unique children id given by its parent.
        (if isEnd then "0." else parent.id.getAndIncrementNumChildren() + ".")
  }

  private[mccct] def getAndIncrementNumChildren(): Int = numChildren.incrementAndGet()

  private[mccct] def getNumChildren(): Int = numChildren.get()

  def getId(): String = id

  private[mccct] def reset(): Unit = numChildren = AtomicInteger(0)

  def getParent(): String = parent.id.getId()

object Id {

  def isChildrenInList(list: List[Controller], parent: Controller): Boolean =
    val parentId = parent.id.getId()
    list.exists { c =>
      val childId = c.id.getId()
      childId != parentId && childId.startsWith(parentId)
    }

}
