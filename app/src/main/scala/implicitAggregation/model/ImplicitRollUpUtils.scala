package implicitAggregation.model

import org.apache.hadoop.shaded.org.eclipse.jetty.util.IO

import java.io.{BufferedWriter, File, FileWriter}

object ImplicitRollUpUtils {

  private val ABSOLUTE_PROJECT_PATH: String = System.getProperty("user.dir")
  val SCENARIOS_PATH: String = ABSOLUTE_PROJECT_PATH + "/app/src/main/resources/scenarios/"
  private val DATA_PATH: String = ABSOLUTE_PROJECT_PATH + "/data"
  private val SQLITE_PATH: String = DATA_PATH + "/sqlite"
  val CUSTOM_CONFIGURATION_FILES: Set[String] = Set(
    "global_graph.txt","mappings.txt","queries.txt","source_graph.txt","wrappers_files.txt"
  )
  val FIXED_CONFIGURATION_FILES: Set[String] = Set(
    "metamodel.txt","prefixes.txt"
  )
  val buildScenarioPath: String => String = scenario => SCENARIOS_PATH + scenario + "/"

  def buildPath(scenario: String): Unit = {
    new File(SCENARIOS_PATH).mkdir()
    new File(buildScenarioPath(scenario)).mkdir()
    new File(DATA_PATH).mkdir()
    new File(SQLITE_PATH).mkdir()
    CUSTOM_CONFIGURATION_FILES.foreach(fileName => new File(buildScenarioPath(scenario) + fileName).createNewFile())
  }

  /**
   * TODO: The query is not the expanded one
   * @param multidimensionalGraph
   * @param wrappers
   * @param query
   * @param scenario
   */
  def generateAllFiles(multidimensionalGraph: Set[Concept], wrappers: Set[Wrapper], queries: List[Concept])(scenario: String): Unit = {
    buildPath(scenario)
    val scenario_path = buildScenarioPath(scenario)
    println("Generating files in directory: " + scenario_path)
    // Writers
    val globalGraphWriter = new BufferedWriter(new FileWriter(scenario_path + "global_graph.txt"))
    val sourceGraphWriter = new BufferedWriter(new FileWriter(scenario_path + "source_graph.txt"))
    val wrapperWriter = new BufferedWriter(new FileWriter(scenario_path + "wrappers_files.txt"))
    val mappingWriter = new BufferedWriter(new FileWriter(scenario_path + "mappings.txt"))
    val queryWriter = new BufferedWriter(new FileWriter(scenario_path + "queries.txt"))

    // Generating global graph files
    globalGraphWriter.write(multidimensionalGraph.map(_.stringify()).foldRight("")(_ + _))
    // generating files related to wrappers
    sourceGraphWriter.write(wrappers.map(_.stringify()).foldRight("")(_ + _))
    wrappers.foreach(w => {
      //      Wrapper.GenerateMappingsFile(w,mappingWriter,scenario)
      Wrapper.GenerateWrapperFile(w,wrapperWriter,scenario_path)
      // Generating data files for each wrapper
      if(!new File(scenario_path + w.name +".csv").exists()){
        val csvWriter = new BufferedWriter(new FileWriter(scenario_path + w.name +".csv"))
        Wrapper.GenerateCsv(w,csvWriter,scenario_path)
        csvWriter.close()
      }
    })
    val it: Iterator[Int] = new Iterator[Int] {
      var c = 0
      override def hasNext: Boolean = true
      override def next(): Int = {c = c + 1;c}
    }
    val SPARQL = queries.foldRight("#"){(q,acc) => acc + Query.generateQueries(q)(it)}
    queryWriter.write(SPARQL)
    // Closing
    globalGraphWriter.close()
    sourceGraphWriter.close()
    wrapperWriter.close()
    Mapping.generateMappingFile(wrappers)(multidimensionalGraph.head)(scenario_path + "global_graph.txt")(mappingWriter)
    mappingWriter.close()
    queryWriter.close()

    copyF(new File("app/src/main/resources/configFiles/metamodel.txt"), scenario_path + "metamodel.txt")
    copyF(new File("app/src/main/resources/configFiles/prefixes.txt"), scenario_path + "prefixes.txt")
  }

  def copyF(from: java.io.File, to: String): Unit =  {
    IO.copy(from,new File(to))
  }

}
