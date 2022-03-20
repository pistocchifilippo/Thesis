import implicitAggregation.experimentation.Experiment

import java.io.{BufferedWriter, FileWriter}

object Application extends App {
  val tolerance = 20000 // Affects the time of each REWRITE call, but we don't study this because we already know from paper
  val exp = 1

  // 1,2,3,4,5,6,7,8,9,10 (2)
  val nDim = 2
  // 1,2,3,4,5,6,7,8,9,10 (2)
  val nLev = 2
  val nWra = 10
  val nRollUpQ = Math.pow(nLev,nDim).toInt
  val nLutWra = nDim * (nLev - 1)
  val nDataWra = Math.min(tolerance,nRollUpQ*nWra)
  val nTotWra = nLutWra + nDataWra
  val r: (Long, Long, Long) = new Experiment(nDim, nLev, nWra, tolerance).runExperiment()
  println(r)

  val toPrint = (exp,nDim,nLev,nWra,nRollUpQ,nLutWra,nTotWra,nDataWra,r._1,r._2,r._3)
  val globalGraphWriter = new BufferedWriter(new FileWriter("experiments.txt",true))
  val line = toPrint.toString().substring(1,toPrint.toString().length-1)
  globalGraphWriter.append("\n" + line)
  globalGraphWriter.close()

}
