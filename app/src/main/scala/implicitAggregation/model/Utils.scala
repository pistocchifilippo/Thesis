package implicitAggregation.model

import org.apache.hadoop.shaded.org.eclipse.jetty.util.IO

import java.io.{BufferedWriter, File, FileWriter}

object Utils {

  val ABSOLUTE_PROJECT_PATH: String = System.getProperty("user.dir")
  val SCENARIOS_PATH: String = ABSOLUTE_PROJECT_PATH + "/app/src/main/resources/scenarios/"
  private val DATA_PATH: String = ABSOLUTE_PROJECT_PATH + "/data"
  private val SQLITE_PATH: String = DATA_PATH + "/sqlite"
  private val CONFIGURATION_FILES: Set[String] = Set(
    "global_graph.txt","source_graph.txt","wrappers_files.txt","mappings.txt","queries.txt"
  )
  val buildScenarioPath: String => String = scenario => SCENARIOS_PATH + scenario + "/"

  def buildPath(scenario: String): Unit = {
    new File(SCENARIOS_PATH).mkdir()
    new File(buildScenarioPath(scenario)).mkdir()
    new File(DATA_PATH).mkdir()
    new File(SQLITE_PATH).mkdir()
    CONFIGURATION_FILES.foreach(fileName => new File(buildScenarioPath(scenario) + fileName).createNewFile())
  }

  def generateAllFiles(concepts: Set[Concept], wrappers: Set[Wrapper], query: Concept)(scenario: String): Unit = {
    val scenario_path = buildScenarioPath(scenario)
    println("Generating files in directory: " + scenario_path)
    // Writers
    val globalGraphWriter = new BufferedWriter(new FileWriter(scenario_path + "global_graph.txt"))
    val sourceGraphWriter = new BufferedWriter(new FileWriter(scenario_path + "source_graph.txt"))
    val wrapperWriter = new BufferedWriter(new FileWriter(scenario_path + "wrappers_files.txt"))
    val mappingWriter = new BufferedWriter(new FileWriter(scenario_path + "mappings.txt"))
    val queryWriter = new BufferedWriter(new FileWriter(scenario_path + "queries.txt"))

    // Generating global graph files
    globalGraphWriter.write(concepts.map(_.stringify()).foldRight("")(_ + _))
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
    queryWriter.write(Query.generateQuery(query))
    // Closing
    globalGraphWriter.close()
    sourceGraphWriter.close()
    wrapperWriter.close()
    Mapping.generateMappingFile(wrappers)(concepts.head)(scenario_path + "global_graph.txt")(mappingWriter)
    mappingWriter.close()
    queryWriter.close()

    copyF(new File("app/src/main/resources/configFiles/metamodel.txt"), scenario_path + "metamodel.txt")
    copyF(new File("app/src/main/resources/configFiles/prefixes.txt"), scenario_path + "prefixes.txt")
  }

  def copyF(from: java.io.File, to: String) {
    IO.copy(from,new File(to))
  }

}
