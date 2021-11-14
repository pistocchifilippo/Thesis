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

  def makeView(): String = "SELECT a,b,c\nFROM A"

  def makeSqlQuery(functions:Set[AggregatingFunction],q:Concept): String =
    if(canAggregate(q)){
      val gbClauses = parseGBClauses(extractGroupByClauses(q))
      val aggClauses = parseAggregationClauses(extractAggregationClauses(functions,q))
      val view = makeView()
      s"SELECT $gbClauses,$aggClauses\nFROM{\n$view\n}\nGROUP BY $gbClauses"
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
  val q =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
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

  val avg = AggregatingFunction("avg") aggregates REVENUE
  val sum = AggregatingFunction("sum") aggregates REVENUE

//  println(
//    extractAggregationClauses(Set(avg,sum),q).map(e => (e._1.name,e._2))
//  )
  println(makeSqlQuery(Set(avg,sum),q))


}
