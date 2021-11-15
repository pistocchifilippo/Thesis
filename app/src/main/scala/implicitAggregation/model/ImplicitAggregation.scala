package implicitAggregation.model

import implicitAggregation.model.ImplicitAggregation._

object ImplicitAggregation {

  def canAggregate(q:Concept): Boolean =  allLevels(q).nonEmpty && allMeasures(q).nonEmpty && !allFeatures(q).exists(f => f match {
    case _: Measure => false
    case _: IdFeature => false
    case _ => true
  })

  def extractGroupByClauses(q:Concept): Set[Level] = allLevels(q)

  def extractAggregationClauses(functions:Set[AggregatingFunction], q:Concept): Set[(AggregatingFunction,Set[Measure])] = {
    val measures = allMeasures(q)
    functions.map(f => (f,f.measures.intersect(measures)))
  }

  def parseGBClauses(levels: Set[Level]): String = levels.map(_.name).mkString(",")

  def parseAggregationClauses(functionAndMeasure:Set[(AggregatingFunction,Set[Measure])]): String =
    functionAndMeasure.flatMap(e => e._2.map(c => s"${e._1.name}(${c.name}) as ${c.name}")).mkString(",")

  def aggregationLevels(q: Concept)(trace: Set[Level]): Set[Level] = q match {
    case l:Level => {
      if (!l.linkedFeatures.exists(f => f._2 match {
        case IdFeature(_) => true
        case _ => false
      })) Set.empty ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(trace + l)) else Set(l) ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(Set.empty)) ++ trace
    } ++ q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(trace + l))
    case _ => q.linkedConcepts.flatMap(c => aggregationLevels(c._2)(Set.empty))
  }

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

  def allConcept(query: Concept): Set[Concept] =
    if(query.linkedConcepts.isEmpty) Set(query) else query.linkedConcepts.flatMap(c => allConcept(c._2)) + query

  def allLevels(query: Concept): Set[Level] =
    query match {
      case l: Level => if (query.linkedConcepts.nonEmpty) query.linkedConcepts.flatMap(c => allLevels(c._2)) + l else Set(l)
      case _ => if(query.linkedConcepts.nonEmpty) query.linkedConcepts.flatMap(c => allLevels(c._2)) else Set.empty
    }

  def allFeatures(query: Concept): Set[Feature] =
    query.linkedFeatures.map(_._2) ++ {
      if (query.linkedConcepts.nonEmpty) query.linkedConcepts.flatMap(c => allFeatures(c._2)) else Set.empty
    }

  def allMeasures(query: Concept): Set[Measure] =
    query.linkedFeatures.collect(f => f._2 match {
      case m:Measure => m
    }) ++ {
      if (query.linkedConcepts.nonEmpty) query.linkedConcepts.flatMap(c => allMeasures(c._2)) else Set.empty
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

//  println(
//    extractAggregationClauses(Set(avg,sum),q).map(e => (e._1.name,e._2))
//  )

  val v = aggregationLevels(q2)(Set.empty)
  println(v.map(_.name))
  println(findFeature(v)(g))


}
