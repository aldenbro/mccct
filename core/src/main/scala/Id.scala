package mccct

import java.util.concurrent.atomic.AtomicInteger

class Id(val parent: Controller, val isEnd: Boolean = false):

  var numChildren: AtomicInteger = AtomicInteger(0)
  private val id: String         = {
    // We use parent as prefix, if parent exists and isn't root
    val prefix =
      if (parent == null || parent.isRoot)
        ""
      else
        parent.id.getId()

    // We use the unique children index for a normal controller.
    // If it is a end controller it gets the special "0." suffix.
    // The id of root is just "0.", a special case.
    val suffix =
      if (parent == null || isEnd)
        "0."
      else
        parent.id.getAndIncrementNumChildren() + "."

    prefix + suffix
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
