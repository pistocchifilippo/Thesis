package implicitAggregation.model

object ImplicitRollUp {

  def executeImplicitRollUp(functions:Set[AggregatingFunction],query: Concept, graph: Concept,scenario:String,basePath:String)(makeImplicitRollUp: Boolean)(wrappers: Set[Wrapper]): String = {
    if (canAggregate(query) && makeImplicitRollUp) {
      println("IMPLICIT ROLL-UP: YES")
      val allDimensionQueries_ = allDimensionQueries(query, graph)
      val rollUpQueries_ = query :: rollUpQueries(query, allDimensionQueries_)
      ImplicitRollUpUtils.generateAllFiles(Set(graph), wrappers, rollUpQueries_)(scenario)
      val CQs = Rewriting.rewrite(scenario, basePath)
      CQs.foreach(println)
      val sql = CQs.map(Rewriting.toSQL(_)(Utils.buildScenarioPath(scenario))(wrappers))
      val gbClauses = parseGBClauses(extractGroupByClauses(query))
      val aggClauses = parseAggregationClauses(extractAggregationClauses(functions, query))
      val gbSql = sql.map(wrapGroupBy(_, gbClauses, aggClauses))
      gbSql.foreach(println)
      val unionSql = gbSql.tail.foldRight(gbSql.head)((a, b) => b + "\nUNION\n" + a)
      println()
      println(unionSql)
//      Rewriting.executeSql(CQs.last,unionSql)
      null
    } else {
      println("IMPLICIT ROLL-UP: NO")
      ImplicitRollUpUtils.generateAllFiles(Set(graph), wrappers, List(query))(scenario)
      val CQ = Rewriting.rewrite(scenario, basePath).head
      val sql = Rewriting.toSQL(CQ)(Utils.buildScenarioPath(scenario))(wrappers)
      Rewriting.executeSql(CQ, sql)
      null
    }
  }

  def canAggregate(query: Concept): Boolean = extractGroupByClauses(query).nonEmpty && Graph.allMeasures(query).nonEmpty
//  def canAggregate(query: Concept): Boolean = extractGroupByClauses(query).nonEmpty && Graph.allMeasures(query).nonEmpty && Graph.allConcept(query).map {
//    case l: Level =>
//      !l.linkedFeatures.map(_._2).exists(x => x match {
//        case _: Measure => false
//        case _: IdFeature => false
//        case _ => true
//      })
//    case c => !c.linkedFeatures.map(_._2).exists(x => x match {
//      case _: Measure => false
//      case _ => true
//    })
//  }.foldRight(true)(_ && _)

  /**
   * Assumes there is just one [[IdFeature]] for each level and that the last queried level contains and [[IdFeature]].
   * @param q The query
   * @return All the [[Level]]s in a query having an [[IdFeature]]
   */
  def extractGroupByClauses(query: Concept): Set[Level] = allDimensions(query).map(d => higherGranularityLevel(d._2)).filter(l => l.linkedFeatures.exists(f => f._2 match {
    case _:IdFeature => true
    case _ => false
  }))

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
   * Detach all the dimensions from the query
   * @param query
   * @return The query without the dimensions
   */
  def pruneQueryDimensions(query: Concept): Concept = Concept.copyConcept(query)(query.linkedConcepts.filter(e => e._2 match {
    case _:Level => false
    case _ => true
  }))(query.linkedFeatures)

  /**
   * Extracts the entire dimension from the graph
   * @param level is the dimension name that will be extracted
   * @param graph
   * @return
   */
  def extractDimension(level: Level, graph: Concept): Option[Level] = graph.linkedConcepts.map(_._2).collect{case l:Level => l}.find(_.name == level.name)

  /**
   *
   * @param prunedGraphDimension the dimension in the graph pruned and aligned to the query dimension
   * @return all the dimension combination
   */
  def generateDimensionQueries(prunedGraphDimension: Level): Set[Level] = {
    val nextPrunedId = removeFirstRemovableId(prunedGraphDimension)
    Set(prunedGraphDimension) ++ {
      if (higherGranularityLevel(nextPrunedId).linkedFeatures.isEmpty) Set.empty else
        Set(nextPrunedId) ++ generateDimensionQueries(nextPrunedId)
    }
  }

  def removeFirstRemovableId(dimension: Level): Level = {
    {
      if (dimension.linkedFeatures.isEmpty && dimension.linkedConcepts.nonEmpty) {
        Concept.copyConcept(dimension)(Set((Edge("partOf"), removeFirstRemovableId(dimension.linkedConcepts.head._2.asInstanceOf[Level]))))(Set.empty)
      } else {
        Concept.copyConcept(dimension)(dimension.linkedConcepts)(Set.empty)
      }
    }.asInstanceOf[Level]

  }

  def allDimensionQueries(query: Concept, graph: Concept): List[List[Level]] = {
    val queryDimensions = allDimensions(query)

    // The dimension query
    val allDimensionQueries = queryDimensions.map(d => {
      val queryDimension = d._2
      val graphDimension = extractDimension(queryDimension, graph)
      val prunedGraphDimension = pruneGraphDimension(queryDimension,graphDimension.get)
      val queries = generateDimensionQueries(prunedGraphDimension)
      (d._1,queries)
    })

    val l = allDimensionQueries.map(q => q._2.toList).toList
    l
  }

  def rollUpQueries(query: Concept, dimensionQueries: List[List[Concept]]): List[Concept] = {
    def dimensions = allDimensions(query)
    def crossJoin[T](list: List[List[T]]): List[List[T]] =
      list match {
        case xs :: Nil => xs map (List(_))
        case x :: xs => for {
          i <- x
          j <- crossJoin(xs)
        } yield List(i) ++ j
      }

    val detachedQuery = pruneQueryDimensions(query)

    val cross = crossJoin(dimensionQueries)

    val rollUpQueries = cross.map(q => Concept.copyConcept(detachedQuery)(q.map(e => (dimensions.find(x => x._2.name == e.name).get._1,e.asInstanceOf[Concept])).toSet)(detachedQuery.linkedFeatures))

    rollUpQueries
  }

  /**
   * Assume each level has just one partOf relationship
   * @param queryDimension
   * @param graphDimension
   * @return
   */
  def pruneGraphDimension(queryDimension: Level, graphDimension: Level): Level = {
    val prunedLinkedConceptsQ = pruneLinkedConcepts(queryDimension.linkedConcepts)
    val prunedLinkedConceptsG = pruneLinkedConcepts(graphDimension.linkedConcepts)

    val nextLevelQ = prunedLinkedConceptsQ.map(_._2).headOption.asInstanceOf[Option[Level]]
    val nextLevelG = prunedLinkedConceptsG.map(_._2).headOption.asInstanceOf[Option[Level]]

    if (nextLevelQ.nonEmpty) {
      Concept.copyConcept(graphDimension)(Set((Edge("partOf"),pruneGraphDimension(nextLevelQ.get,nextLevelG.get).asInstanceOf[Concept])))(graphDimension.linkedFeatures.filter(e => e._2 match {
        case _:IdFeature => true
        case _ => false
      })).asInstanceOf[Level]
    } else
      Concept.copyConcept(graphDimension)(Set.empty)(graphDimension.linkedFeatures.filter(e => e._2 match {
        case _:IdFeature => true
        case _ => false
      })).asInstanceOf[Level]
  }

  def pruneLinkedConcepts(linkedConcepts: Set[(Edge, Concept)]): Set[(Edge, Concept)] =
    linkedConcepts.filter(e => e._1.name == "partOf")

  /**
   * Assumes all the dimensions are directly attached to the fact.
   * @param query is the Fact
   * @return all the dimensions graph attached to the fact. The detection is done by checking the attached Level
   *         and then returning the entire branch
   */
  def allDimensions(query: Concept): Set[(Edge,Level)] = query.linkedConcepts.filter(c => c._2 match {
    case _:Level => true
    case _ => false
  }).asInstanceOf[Set[(Edge,Level)]]

  /**
   * Assumes there is only one partOf relationship starting from each level.
   * @param level that is the lower granularity level of a dimension
   * @return The higher granularity level of the dimension
   */
  def higherGranularityLevel(level: Level): Level =
    if (!level.linkedConcepts.map(_._1).exists(e => e.name == "partOf")) level
    else higherGranularityLevel(level.linkedConcepts.filter(e => e._1.name == "partOf").map(_._2).head.asInstanceOf[Level])

  def wrapGroupBy(sql: String, gbClauses: String, aggClauses: String): String = s"SELECT $gbClauses,$aggClauses\nFROM( $sql )\nGROUP BY $gbClauses"

}