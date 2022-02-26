import implicitAggregation.experimentation.Experiment

import java.io.{BufferedWriter, FileWriter}

object Application extends App {
  val nDim = 5
  val nLev = 2
  val nWra = 1
  val r: (Long, Long, Long) = new Experiment(nDim, nLev, nWra).runExperiment()
  println(r)
  val globalGraphWriter = new BufferedWriter(new FileWriter("experiments.txt"))
  globalGraphWriter.write(r.toString())
  globalGraphWriter.close()

}
