package mccct

trait FailureExplorationAlgorithm:
  def shouldInject(): Boolean

object NeverInject extends FailureExplorationAlgorithm:
  def shouldInject(): Boolean = false

object AlwaysInject extends FailureExplorationAlgorithm:
  def shouldInject(): Boolean = true

class InjectRandomly(chance: Double) extends FailureExplorationAlgorithm:
  def shouldInject(): Boolean =
    scala.util.Random.nextDouble() < chance
