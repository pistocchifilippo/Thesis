package implicitAggregation.model

import edu.upc.essi.dtim.nextiaqr.models.querying.{ConjunctiveQuery, RewritingResult}
import edu.upc.essi.dtim.{NextiaQR, TestUtils}
import org.apache.commons.io.FileUtils
import org.apache.jena.query.ReadWrite
import org.apache.jena.tdb.TDBFactory

import java.io.File
import java.util

object Rewriting {

  def rewrite(scenario: String, basePath: String): List[RewritingResult] = {
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

    val CQs = {
      if (queries.size() == 1) {
        val query = queries.remove(0)
        println()
        println("QUERY: " + query._2)
        println("MINIM: " + query._2)
        val cq = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T, query._2)
        println(cq.getCQs)
        List(cq)
      } else {
        val minimal = queries.remove(0)
        import collection.JavaConverters._
        queries.asScala.map(query => { //.sortWith((t1,t2) => t1._2.length < t2._2.length)
          println()
          println("QUERY: " + query._2)
          println("MINIM: " + minimal._2)
          val cq = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T, minimal._2)
          println(cq.getCQs)
          cq
        }).toList
      }
    }.filter(!_.getCQs.isEmpty)

//    CQs.foreach(println)
    CQs
  }

  def toSQL(CQs: RewritingResult)(scenarioPath: String)(wrappers: Set[implicitAggregation.model.Wrapper]): String = {
    NextiaQR.toSQL(CQs, makeNameMappings(wrappers))
  }

  def executeSql(CQs: java.util.Set[ConjunctiveQuery], sql: String): Unit = NextiaQR.executeSQL(CQs,sql)

  def flatten(CQs: List[RewritingResult]): java.util.Set[ConjunctiveQuery] = {
    import collection.JavaConverters._
    CQs.flatMap(s => s.getCQs.asScala).toSet.asJava
  }

  def pruneDoubleCQs(CQs: List[RewritingResult])(cachedCQs: Set[ConjunctiveQuery]): List[RewritingResult] = {
    import collection.JavaConverters._
    CQs match {
      case h :: t => {
        val newCached = cachedCQs ++ h.getCQs.asScala
        h.setCQs(h.getCQs.asScala.filter(e => !cachedCQs.contains(e)).asJava)
        h :: pruneDoubleCQs(t)(newCached)
      }
      case Nil => List.empty
    }
  }

  //jc.getLeft_attribute.contains("_lut_") || jc.getRight_attribute.contains("_lut_")
  def filterWrapperToWrapperJoin(CQs: List[RewritingResult]): List[RewritingResult] = {
    import collection.JavaConverters._
    CQs.map(
      rew => {
        rew.setCQs(
          rew.getCQs.asScala.filter(cq =>
            cq.getJoinConditions.asScala.foldRight(true)((jc,state) => {jc.getLeft_attribute.contains("_lut_") || jc.getRight_attribute.contains("_lut_")} && state)
          ).asJava
        )
        rew
      }
    ).filter(rew => !rew.getCQs.isEmpty)
  }

  private def makeNameMappings(wrappers: Set[implicitAggregation.model.Wrapper]): util.Map[String,String] = {
    val map: util.Map[String,String] = new util.HashMap()
    wrappers.flatMap(w => w.attributes).filter(a => a.sameAs.nonEmpty).map(a => (a.name, a.sameAs.get.name)).foreach(a => map.put(a._1,a._2))
    map
  }

}
