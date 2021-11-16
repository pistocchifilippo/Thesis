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

object QueryExecution {

  def execute(scenario:String,basePath:String,makeImplicitAggregation: Boolean)(q: Concept)(functions:Set[AggregatingFunction])(wrappers: Set[implicitAggregation.model.Wrapper]): Unit = {
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

    queries.forEach(query => {
      println(query._1)

      //1 -- Rewrite SPARQL to UCQs
      val CQs = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T)
      println(CQs)

      //2 -- Convert UCQs to SQL
      /**
       * Here we assume all wrappers are CSV and there exists a file 'wrappers_files.txt' that
       * maps the wrapper IRI  a file path
       */
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
      val SQL = if(makeImplicitAggregation)
        ImplicitAggregation.makeSqlQuery(functions, q, () => NextiaQR.toSQL(CQs, makeNameMappings(wrappers)))
      else
        NextiaQR.toSQL(CQs,null)

      //3 -- Convert SQL to DATA
      NextiaQR.executeSQL(CQs, SQL)
    })
  }

  def makeNameMappings(wrappers: Set[implicitAggregation.model.Wrapper]): util.Map[String,String] = {
    val map: util.Map[String,String] = new util.HashMap()
    wrappers.flatMap(w => w.attributes).filter(a => a.sameAs.nonEmpty).map(a => (a.name, a.sameAs.get.name)).foreach(a => map.put(a._1,a._2))
    map
  }
}
