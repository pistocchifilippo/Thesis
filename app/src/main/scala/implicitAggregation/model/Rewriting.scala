package implicitAggregation.model

import com.google.common.collect.{Maps, Sets}
import edu.upc.essi.dtim.nextiaqr.models.querying.wrapper_impl.CSV_Wrapper
import edu.upc.essi.dtim.nextiaqr.models.querying.{ConjunctiveQuery, Wrapper}
import edu.upc.essi.dtim.{NextiaQR, TestUtils}
import org.apache.commons.io.FileUtils
import org.apache.jena.query.ReadWrite
import org.apache.jena.tdb.TDBFactory

import java.io.File
import java.nio.file.Files
import java.util

object Rewriting {

  def rewrite(scenario: String, basePath: String): List[java.util.Set[ConjunctiveQuery]] = {
    val baseURI = "http://www.essi.upc.edu/~snadal/" + scenario
    val scenarioPath = basePath + scenario + "/"
    val jenaPath = "TestScenarioRunnerDataset"
    FileUtils.deleteDirectory(new File(jenaPath))

    val prefixes: util.Map[String, String] = TestUtils.populatePrefixes(scenarioPath + "prefixes.txt")
    TestUtils.populateTriples(jenaPath, baseURI, scenarioPath + "metamodel.txt", prefixes)
    TestUtils.populateTriples(jenaPath, baseURI, scenarioPath + "global_graph.txt", prefixes)
    TestUtils.populateTriples(jenaPath, baseURI, scenarioPath + "source_graph.txt", prefixes)
    TestUtils.populateMappings(jenaPath, scenarioPath + "mappings.txt", scenarioPath + "global_graph.txt", prefixes)

    val queries = TestUtils.getQueries(scenarioPath + "queries.txt", prefixes)
    val T = TDBFactory.createDataset(jenaPath)
    T.begin(ReadWrite.READ)

    val CQs = if (queries.size() == 1) {
      val query = queries.remove(0)
      println()
      println("QUERY: " + query._2)
      println("MINIM: " + query._2)
      val cq = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T,query._2)
      println(cq)
      List(cq)
    } else {
      val minimal = queries.remove(0)
      import collection.JavaConverters._
      queries.asScala.sortWith((t1,t2) => t1._2.length < t2._2.length).sortWith((t1,t2) => t1._2.length < t2._2.length).map(query => { //.sortWith((t1,t2) => t1._2.length < t2._2.length)
        println()
        println("QUERY: " + query._2)
        println("MINIM: " + minimal._2)
        val cq = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T, minimal._2)
        println(cq)
        cq
      }).toList
    }

//    CQs.foreach(println)
    CQs
  }

  def toSQL(CQs: java.util.Set[ConjunctiveQuery])(scenarioPath: String)(wrappers: Set[implicitAggregation.model.Wrapper]): String = {
    val iriToCSVPath:util.Map[String,String] = Maps.newHashMap
    Files.readAllLines(new File(scenarioPath + "wrappers_files.txt").toPath).stream.forEach((s: String) => {
      iriToCSVPath.put(s.split(",")(0), s.split(",")(1))
    })
    CQs.forEach((cq: ConjunctiveQuery) => {
      val CSV_Wrappers: util.Set[Wrapper] = Sets.newHashSet
      cq.getWrappers.forEach((w: Wrapper) => {
        val csv = new CSV_Wrapper(w.getWrapper)
        csv.setPath(iriToCSVPath.get(w.getWrapper))
        csv.setHeaderInFirstRow(true)
        csv.setColumnDelimiter(",")
        csv.setRowDelimiter("\n")
        CSV_Wrappers.add(csv)
      })
      cq.setWrappers(CSV_Wrappers)
    })
    NextiaQR.toSQL(CQs, makeNameMappings(wrappers))
  }

  def executeSql(CQs: java.util.Set[ConjunctiveQuery], sql: String): Unit = NextiaQR.executeSQL(CQs,sql)

  def flatten(CQs: List[java.util.Set[ConjunctiveQuery]]): java.util.Set[ConjunctiveQuery] = {
    import collection.JavaConverters._
    CQs.flatMap(s => s.asScala).toSet.asJava
  }

  private def makeNameMappings(wrappers: Set[implicitAggregation.model.Wrapper]): util.Map[String,String] = {
    val map: util.Map[String,String] = new util.HashMap()
    wrappers.flatMap(w => w.attributes).filter(a => a.sameAs.nonEmpty).map(a => (a.name, a.sameAs.get.name)).foreach(a => map.put(a._1,a._2))
    map
  }

}
