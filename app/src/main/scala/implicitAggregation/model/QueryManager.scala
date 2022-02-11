package implicitAggregation.model

import edu.upc.essi.dtim.nextiaqr.models.querying.RewritingResult
import edu.upc.essi.dtim.{NextiaQR, TestUtils}
import org.apache.commons.io.FileUtils
import org.apache.jena.query.{Dataset, ReadWrite}
import org.apache.jena.tdb.TDBFactory

import java.io.File
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

  def toSql(CQs: RewritingResult, scenarioPath: String, makeImplicitAggregation: Boolean)(wrappers: Set[implicitAggregation.model.Wrapper]): String = {
    val SQL = if(makeImplicitAggregation) {
      NextiaQR.toSQL(CQs, makeNameMappings(wrappers))
    } else {
      NextiaQR.toSQL(CQs, null)
    }
    SQL
  }

  def executeRollUpQueries(q: java.util.List[edu.upc.essi.dtim.nextiaqr.utils.Tuple2[String,String]], T: Dataset,makeImplicitRollUp: Boolean): List[RewritingResult] = {
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
