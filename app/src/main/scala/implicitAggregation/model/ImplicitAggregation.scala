package implicitAggregation.model

/**
 * This source file is containing the main step of the Implicit Aggregation algorithm
 */
object ImplicitAggregation {

  /**
   *
   * @param q The query
   * @return true if there is at least a GB clause, a measure and there aren't any [[GenericFeature]]
   *         TODO: There may be IdFeature non liked to a level that shouldn't be accepte
   */
  def canAggregate(q:Concept): Boolean =  extractGroupByClauses(q).nonEmpty && Graph.allMeasures(q).nonEmpty && Graph.allConcept(q).map(c => c match {
    case l:Level =>
      l.linkedFeatures.map(_._2).filter(x => x match {
      case _: Measure => false
      case _: IdFeature => false
      case _ => true
    }).isEmpty
    case _ => c.linkedFeatures.map(_._2).filter(x => x match {
      case _: Measure => false
      case _ => true
    }).isEmpty
  }).foldRight(true)(_ && _)
//    !Graph.allFeatures(q).exists(f => f match {
//    case _: Measure => false
//    case _: IdFeature => false
//    case _ => true
//  })

  /**
   *
   * @param q The query
   * @return All the [[Level]]s in a query having an [[IdFeature]]
   */
  def extractGroupByClauses(q:Concept): Set[Level] = q match {
    case l:Level => q.linkedConcepts.flatMap(c => extractGroupByClauses(c._2)) ++ {
      if (Graph.getIdFromConcept(l).nonEmpty) Set(l) else Set.empty
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

  /**
   *
   * @param levels of aggregation
   * @return A [[String]] containing the GroupBy clauses in SQL syntax
   */
  def parseGBClauses(levels: Set[Level]): String = levels.map(_.name).mkString(",")

  /**
   *
   * @param functionAndMeasure
   * @return A [[String]] containing the aggregation operator in SQL syntax
   */
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

  /**
   * This function contain the instructions flow of the algorithm
   * @param functions
   * @param q
   * @param makeView is a function [[() => String]] that generates the view in SQL syntax
   * @return A [[String]] containing the complete query in SQL syntax
   */
  def makeSqlQuery(functions:Set[AggregatingFunction],q:Concept,makeView:() => String): String =
    if(canAggregate(q)){
      val gbClauses = parseGBClauses(extractGroupByClauses(q))
      val aggClauses = parseAggregationClauses(extractAggregationClauses(functions,q))
      s"SELECT $gbClauses,$aggClauses\nFROM( ${makeView()} )\nGROUP BY $gbClauses"
    } else {
      makeView()
    }


  /**
   * All the [[Level]]'s identifier feature of lower granularity [[Level]]s of the same dimension of the queried one have to be included in the query
   * Let's just expand all the possible features for each level for simplicity and let the algorithm cut the useless one.
   * @param query
   * @param graph
   * @return The expanded query
   */
  def expandAggregationLevels(query: Concept)(graph: Concept): Concept = {
    val concepts = query.linkedConcepts.flatMap(c => {
      graph.linkedConcepts.find(c2 => c2._2.name == c._2.name) match {
        case Some(a) => Set((c,a))
        case None => Set.empty
      }
    })
    Concept.copyConcept(query)(concepts.map(x => (x._1._1,expandAggregationLevels(x._1._2)(x._2._2))))(graph.linkedFeatures)
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
      .hasFeature(REGION)
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

  println(ImplicitAggregation.canAggregate(q2))

  println(Graph.allMeasures(q2).map(_.name))
  println(
    Graph.allMeasures(
      Graph.copyAllGraph(q2)
    ).map(_.name)
  )
}
