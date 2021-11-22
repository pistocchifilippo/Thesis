package implicitAggregation.model

import implicitAggregation.model.ImplicitAggregation._

object ImplicitAggregation {

  /**
   *
   * @param q The query
   * @return true if there is at least a GB clause, a measure and there aren't any [[GenericFeature]]
   *         TODO: There may be IdFeature non liked to a level that shouldn't be accepted
   */
  def canAggregate(q:Concept): Boolean =  extractGroupByClauses(q).nonEmpty && Graph.allMeasures(q).nonEmpty && !Graph.allFeatures(q).exists(f => f match {
    case _: Measure => false
    case _: IdFeature => false
    case _ => true
  })

  /**
   *
   * @param q The query
   * @return All the [[Level]]s in a query having an [[IdFeature]]
   */
  def extractGroupByClauses(q:Concept): Set[Level] = q match {
    case l:Level => q.linkedConcepts.flatMap(c => extractGroupByClauses(c._2)) ++ {
      if (getIdFromConcept(l).nonEmpty) Set(l) else Set.empty
    }
    case _ => q.linkedConcepts.flatMap(c => extractGroupByClauses(c._2))
  }

  /**
   *
   * @param functions The aggregating functions
   * @param q The query
   * @return All the [[AggregatingFunction]] and their related [[Measure]]
   */
  def extractAggregationClauses(functions:Set[AggregatingFunction], q:Concept): Set[(AggregatingFunction,Set[Measure])] = {
    val measures = Graph.allMeasures(q)
    functions.map(f => (f,f.measures.intersect(measures)))
  }

  def parseGBClauses(levels: Set[Level]): String = levels.map(_.name).mkString(",")

  def parseAggregationClauses(functionAndMeasure:Set[(AggregatingFunction,Set[Measure])]): String =
    functionAndMeasure.flatMap(e => e._2.map(c => s"${e._1.name}(${c.name}) as ${c.name}")).mkString(",")

  /**
   * TODO: Fix after the meeting, I will need this to have the real GB clause
   * @param q
   * @param trace
   * @return
   */
  def aggregationLevels(q: Concept)(trace: Set[Level]): Set[Level] = q match {
    case l:Level => {
      if (!l.linkedFeatures.exists(f => f._2 match {
        case IdFeature(_) => true
        case _ => false
      })) Set.empty ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(trace + l)) else Set(l) ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(Set.empty)) ++ trace
    } ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(trace + l))
    case _ => q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(Set.empty))
  }

  private def getIdFromConcept(concept: Concept): scala.collection.immutable.Set[IdFeature] = concept.linkedFeatures.map(_._2).collect({
    case f:IdFeature => f
  })

//  def expandLevelIdentifiers(aggregationLevels: Set[Level])(q: Concept)(g: Concept): Concept = {
//    q match {
//      case l:Level => Level(l.name)
//      case c:GenericConcept =>
//    }
//  }

  def findFeature(levels: Set[Level])(g: Concept): Set[Feature] = {
    if (g match {
      case l: Level =>
        val ex = l.linkedFeatures.map(_._2).collect({ case f: IdFeature => f }).nonEmpty && levels.map(_.name).contains(g.name)
        println(ex,g.name)
        ex
      case _ => false
    }) g.linkedFeatures.map(_._2).collect({ case f: IdFeature => f }) ++ g.linkedConcepts.flatMap(x => findFeature(levels)(x._2)) else {
      g.linkedConcepts.flatMap(x => findFeature(levels)(x._2))
    }
  }

  def makeSqlQuery(functions:Set[AggregatingFunction],q:Concept,makeView:() => String): String =
    if(canAggregate(q)){
      val gbClauses = parseGBClauses(extractGroupByClauses(q))
      val aggClauses = parseAggregationClauses(extractAggregationClauses(functions,q))
      s"SELECT $gbClauses,$aggClauses\nFROM( ${makeView()} )\nGROUP BY $gbClauses"
    } else {
      makeView()
    }
}

object TestAgg extends App{
  // Features
  val CITY = IdFeature("CITY")
  val REGION = IdFeature("REGION")
  val COUNTRY = IdFeature("COUNTRY")
  val REVENUE = Measure("REVENUE")

  // Graph hierarchy
  val g =
    Concept("Sales")
      .hasFeature{REVENUE}
      .partOf{
        Level("City")
          .hasFeature{CITY}
          .partOf{
            Level("Region")
              .hasFeature{REGION}
              .partOf{
                Level("Country")
                  .hasFeature{COUNTRY}
              }
          }
      }

  val q2 =
    Concept("Sales")
      .hasFeature{REVENUE}
      .partOf{
        Level("City")
          .partOf{
            Level("Region")
              .hasFeature{REGION}
              .partOf{
                Level("Country")
              }
          }
      }

  val avg = AggregatingFunction("avg") aggregates REVENUE
  val sum = AggregatingFunction("sum") aggregates REVENUE

  println(Graph.allMeasures(q2).map(_.name))

}
