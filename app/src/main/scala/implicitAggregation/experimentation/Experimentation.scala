package implicitAggregation.experimentation

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{ImplicitRollUp, Utils}
import org.apache.commons.io.FileUtils

import java.io.File

class Experiment(nDimension: Int, nLevels: Int, nWrappers: Int, tolerance: Int) extends Scenario {
  println(s"nDimension: $nDimension")
  println(s"nLevels: $nLevels")
  println(s"nWrappers: $nWrappers")
  private val experimentsVariable = GraphFactory.generateExperimentsIntegrationGraph(nDimension,nLevels,nWrappers,tolerance)
  super.targetGraph(experimentsVariable._1)
  super.query(experimentsVariable._2)
  experimentsVariable._3.foreach(super.wrapper)
  super.aggregation(experimentsVariable._4)
  //clear directory
  private val scenario = "experiment"
  FileUtils.deleteDirectory(new File(Utils.buildScenarioPath(scenario)))
  super.scenario(scenario)
  println("# Roll-Up queries: " + Math.pow(nLevels,nDimension).toInt)
  def runExperiment(): (Long,Long,Long) =
    ImplicitRollUp.executeImplicitRollUp(AF, makeQuery(), MG, scenarioName, Utils.SCENARIOS_PATH)(makeImplicitRollUp = true)(W)
}
//
//class ExperimentsRunner {
//  private val nDimensions = (List(3,5,6,7),5)
//  private val nLevels = (List(1,2,3,4),2)
//  private val nWrappers = (List(1,2,3,4,5,6,7,8,9,10),1)
//
//  private def dimensionExperiment(): List[(Long,Long,Long,Long,Long,Long)] =
//    nDimensions._1.map(N => {
//      deleteExperimentsDir()
//      val nDim = N
//      val nLev = nLevels._2
//      val nWra = nWrappers._2
//      println(s"EXPERIMENT START nDim:$nDim,nLev:$nLev,nWra:$nWra")
//      val r = new Experiment(N, nLevels._2, nWrappers._2).runExperiment()
//      val out = (N.longValue, nLevels._2.longValue, nWrappers._2.longValue, r._1, r._2, r._3)
//      println(out)
//      out
//    })
//
//  private def levelsExperiment(): List[(Long,Long,Long,Long,Long,Long)] =
//    nLevels._1.map(N => {
//      deleteExperimentsDir()
//      println("EXPERIMENT START")
//      val r = new Experiment(nDimensions._2, N, nWrappers._2).runExperiment()
//      (nDimensions._2, N, nWrappers._2, r._1, r._2, r._3)
//    })
//
//  private def wrappersExperiment(): List[(Long,Long,Long,Long,Long,Long)] =
//    nWrappers._1.map(N => {
//      deleteExperimentsDir()
//      println("EXPERIMENT START")
//      val r = new Experiment(nDimensions._2, nLevels._2, N).runExperiment()
//      (nDimensions._2, nLevels._2, N, r._1, r._2, r._3)
//    })
//
//  private def deleteExperimentsDir(): Unit = {
//    val scenario = "experiment"
//    FileUtils.deleteDirectory(new File(Utils.buildScenarioPath(scenario)))
//
//  }
//
//  def run(): Unit = {
//    dimensionExperiment()
//    //dimensionExperiment() appended levelsExperiment() appended wrappersExperiment()
//  }
//}


//object Experimentation extends App {
//  val r = new Experiment(5, 2, 1,5).runExperiment()
//}
