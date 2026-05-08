import mccct._
import java.{util => ju}
import java.io.PrintWriter

def writeToFile(path: String)(body: PrintWriter => Unit): Unit = {
  val pw = new PrintWriter(path)
  try body(pw)
  finally pw.close()
}

@main
def run() = {
  val iterations = 3

  var configs: Vector[(String, RunMethod)] = Vector()
  (1 to 9).foreach(i => configs = configs :+ (s"0.$i", BasicRun(failureAlg = RandomlyInject(i.toDouble / 10))))

  var vecs: Vector[Vector[(Int, Double)]] = Vector()
  configs.foreach(config =>
    var vec: Vector[(Int, Double)] = Vector()
    (1 to iterations).foreach(x =>
      Benchmark.runUntilCovered(
        benchmark = failureAfterConcurrency(100, 25, 1, 5),
        maxIterations = 1000,
        method = config._2,
        printSummary = false
      )
      vec = vec :+ (Benchmark.iterations, Benchmark.pointsCoveredCount.toDouble / Benchmark.pointsToCoverCount)
      println(f"[INFO]: Iteration complete (${config._1} : $x)")
    )
    vecs = vecs :+ vec
  )

  writeToFile("data_iter.csv") { f =>
    f.println(configs.map(c => c._1).mkString(","))
    (0 until iterations).foreach(i => f.println((0 until configs.size).map(j => f"${vecs(j)(i)._1}").mkString(",")))
  }

  writeToFile("data_rate.csv") { f =>
    f.println(configs.map(c => c._1).mkString(","))
    (0 until iterations).foreach(i => f.println((0 until configs.size).map(j => f"${vecs(j)(i)._2}").mkString(",")))
  }
}

@main
def run_coverage() = {

  val iterations = 3
  // val method     = ImprovedFailureExploration(cctIterations = 2)
  val method = BasicRun(failureAlg = RandomlyInject(0.5))

  object CoverageData {
    var run: Vector[Double] = Vector(.0)
    def reset()             = {
      run = Vector(.0)
    }
  }

  def afterIteration() = {
    CoverageData.run = CoverageData.run :+ (Benchmark.pointsCoveredCount.toDouble / Benchmark.pointsToCoverCount)
  }

  var runs: Vector[Vector[Double]] = Vector()
  var runMaxSize                   = 0

  (1 to iterations).foreach(_ =>
    // We run until coverage have been completed, updating coverage data after each internal iteration
    Benchmark.runUntilCovered(
      benchmark = failureAfterConcurrency(100, 100, 1, 5),
      maxIterations = 1000,
      afterIteration = afterIteration,
      method = method,
      printSummary = false
    )
    // We then adjust the max size (for padding purposes) and append the data, reset for next iteration
    if CoverageData.run.size > runMaxSize then runMaxSize = CoverageData.run.size
    runs = runs :+ CoverageData.run
    CoverageData.reset()

    println(s"Iteration ${runs.size}/$iterations complete")
  )

  // Pad each run to the same length with NaN
  val paddedRuns = runs.map(_.padTo(runMaxSize, Double.NaN))

  // Average ignoring NaN
  def avgIgnoringNaN(values: Seq[Double]): Double = {
    val valid = values.filterNot(_.isNaN)
    if (valid.isEmpty) Double.NaN else valid.sum / valid.size
  }

  // Build rows
  val steps = (0 until runMaxSize)
  val rows  =
    steps.map { i =>
      val values = paddedRuns.map(_(i))
      val avg    = avgIgnoringNaN(values)
      (steps(i), values, avg)
    }

  // Convert to CSV lines
  val header = "steps," + runs.indices.map(i => s"${i + 1}").mkString(",") + ",avg"

  val csvLines = rows.map { case (step, values, avg) =>
    val cols =
      values.map(v => if (v.isNaN) "nan" else f"$v") :+
        (if (avg.isNaN) "nan" else f"$avg")

    (step.toString +: cols).mkString(",")
  }

  writeToFile("data_coverage.csv") { f =>
    f.println(header)
    csvLines.foreach(f.println)
  }
}
