package implicitAggregation.model

import com.google.common.collect.{Maps, Sets}
import edu.upc.essi.dtim.nextiaqr.models.querying.wrapper_impl.CSV_Wrapper
import edu.upc.essi.dtim.nextiaqr.models.querying.{ConjunctiveQuery, Wrapper}
import edu.upc.essi.dtim.{NextiaQR, TestUtils}
import org.apache.commons.io.FileUtils
import org.apache.jena.query.{Dataset, ReadWrite}
import org.apache.jena.tdb.TDBFactory

import java.io.File
import java.nio.file.Files
import java.util

object QueryManager {

  def prepare(scenario: String, basePath: String): (String,Dataset,java.util.List[edu.upc.essi.dtim.nextiaqr.utils.Tuple2[String,String]]) = {
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
    (scenarioPath,T,queries)
  }

  def toSql(CQs: java.util.Set[ConjunctiveQuery], scenarioPath: String, makeImplicitAggregation: Boolean)(wrappers: Set[implicitAggregation.model.Wrapper]): String = {
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
    val SQL = if(makeImplicitAggregation) {
      NextiaQR.toSQL(CQs, makeNameMappings(wrappers))
    } else {
      NextiaQR.toSQL(CQs, null)
    }
    SQL
  }

  def executeRollUpQueries(q: java.util.List[edu.upc.essi.dtim.nextiaqr.utils.Tuple2[String,String]], T: Dataset,makeImplicitRollUp: Boolean): List[util.Set[ConjunctiveQuery]] = {
    import scala.collection.JavaConverters._
    println("Queries: " + q.size())
    if (makeImplicitRollUp) {
      val minimal = q.remove(0)._2
      q.asScala.map(qs => {
        println(qs)
        NextiaQR.rewriteToUnionOfConjunctiveQueries(qs._2,T,qs._2)
      }).toList
    } else {
      val query = q.get(0)
      println(query)
      List(
        NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2,T,query._2)
      )
    }
  }

  /**/

  private def makeNameMappings(wrappers: Set[implicitAggregation.model.Wrapper]): util.Map[String,String] = {
    val map: util.Map[String,String] = new util.HashMap()
    wrappers.flatMap(w => w.attributes).filter(a => a.sameAs.nonEmpty).map(a => (a.name, a.sameAs.get.name)).foreach(a => map.put(a._1,a._2))
    map
  }

}
