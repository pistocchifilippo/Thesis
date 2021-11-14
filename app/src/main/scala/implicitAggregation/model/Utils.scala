package implicitAggregation.model

import org.apache.hadoop.shaded.org.eclipse.jetty.util.IO

import java.io.{BufferedWriter, File, FileWriter}

object Utils {

  def buildPath(path:String): Boolean = {
    new File(path).mkdir()
  }

  def generateAllFiles(concepts: Set[Concept], wrappers: Set[Wrapper], query: Concept)(scenario: String): Unit = {
    println("Generating files in directory: " + scenario)
    // Writers
    val globalGraphWriter = new BufferedWriter(new FileWriter(scenario + "global_graph.txt"))
    val sourceGraphWriter = new BufferedWriter(new FileWriter(scenario + "source_graph.txt"))
    val wrapperWriter = new BufferedWriter(new FileWriter(scenario + "wrappers_files.txt"))
    val mappingWriter = new BufferedWriter(new FileWriter(scenario + "mappings.txt"))
    val queryWriter = new BufferedWriter(new FileWriter(scenario + "queries.txt"))

    // Generating global graph files
    globalGraphWriter.write(concepts.map(_.stringify()).foldRight("")(_ + _))
    // generating files related to wrappers
    sourceGraphWriter.write(wrappers.map(_.stringify()).foldRight("")(_ + _))
    wrappers.foreach(w => {
//      Wrapper.GenerateMappingsFile(w,mappingWriter,scenario)
      Wrapper.GenerateWrapperFile(w,wrapperWriter,scenario)
      // Generating data files for each wrapper
      if(!new File(scenario + w.name +".csv").exists()){
        val csvWriter = new BufferedWriter(new FileWriter(scenario + w.name +".csv"))
        Wrapper.GenerateCsv(w,csvWriter,scenario)
        csvWriter.close()
      }
    })
    queryWriter.write(Query.generateQuery(query))
    // Closing
    globalGraphWriter.close()
    sourceGraphWriter.close()
    wrapperWriter.close()
    Mapping.generateMappingFile(wrappers)(concepts.head)(scenario + "global_graph.txt")(mappingWriter)
    mappingWriter.close()
    queryWriter.close()

    copyF(new File("app/src/main/resources/configFiles/metamodel.txt"), scenario + "metamodel.txt")
    copyF(new File("app/src/main/resources/configFiles/prefixes.txt"), scenario + "prefixes.txt")
  }

  def copyF(from: java.io.File, to: String) {
    IO.copy(from,new File(to))
  }

}
