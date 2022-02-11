package implicitAggregation.model

import edu.upc.essi.dtim.NextiaQR
import edu.upc.essi.dtim.nextiaqr.models.querying.ConjunctiveQuery

import java.util

object QueryExecution {

  def executeSqlQuery(CQs: util.Set[ConjunctiveQuery], query: String): Unit = {NextiaQR.executeSQL(CQs, query)}

  def execute(scenario:String,basePath:String,makeImplicitAggregation: Boolean)(q: Concept)(functions:Set[AggregatingFunction])(wrappers: Set[implicitAggregation.model.Wrapper]): Unit = {
    val p = QueryManager.prepare(scenario,basePath)
    val scenarioPath = p._1
    val T = p._2
    val queries = p._3

    queries.forEach(query => {
//      println(query._1)
//      println("...")

      //1 -- Rewrite SPARQL to UCQs
      val CQs = NextiaQR.rewriteToUnionOfConjunctiveQueries(query._2, T,query._2)
//      println("CQs" + CQs)

      // To SQL
//      val SQL = QueryManager.toSql(CQs,scenarioPath,makeImplicitAggregation)(wrappers)

      //3 -- Convert SQL to DATA
//      NextiaQR.executeSQL(CQs, SQL)
    })
  }

  def makeNameMappings(wrappers: Set[implicitAggregation.model.Wrapper]): util.Map[String,String] = {
    val map: util.Map[String,String] = new util.HashMap()
    wrappers.flatMap(w => w.attributes).filter(a => a.sameAs.nonEmpty).map(a => (a.name, a.sameAs.get.name)).foreach(a => map.put(a._1,a._2))
    map
  }
}
