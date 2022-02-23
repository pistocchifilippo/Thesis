package implicitAggregation.experimentation

import implicitAggregation.model._

object GraphFactory {

  def generateDimension(dName: String, nLevels: Int, cLevel: Int): Concept =
    if (nLevels-1 != 0)
      Level(dName + "L" + cLevel,Set((Edge("partOf"),generateDimension(dName,nLevels-1,cLevel+1))),Set((Edge("hasFeature"),IdFeature(dName + "L" + cLevel))))
    else
      Level(dName + "L" + cLevel,Set.empty,Set((Edge("hasFeature"),IdFeature(dName + "L" + cLevel))))

  def generateDimensionQ(dName: String, nLevels: Int, cLevel: Int): Concept =
    if (nLevels-1 != 0)
      Level(dName + "L" + cLevel,Set((Edge("partOf"),generateDimensionQ(dName,nLevels-1,cLevel+1))),Set.empty)
    else
      Level(dName + "L" + cLevel,Set.empty,Set((Edge("hasFeature"),IdFeature(dName + "L" + cLevel))))

  def generateDimensions(nDim: Int, nLevels: Int): Set[(Edge,Concept)] =
    (1 to nDim).map(n => (Edge("D" + n),generateDimension("D" + n,nLevels,1))).toSet

  def generateDimensionsQ(nDim: Int, nLevels: Int): Set[(Edge,Concept)] =
    (1 to nDim).map(n => (Edge("D" + n),generateDimensionQ("D" + n,nLevels,1))).toSet

  def dimensionFeaturesSorted(dimension: Concept): List[Feature] =
    dimension.linkedFeatures.map(_._2).filter(e => e match {
      case _:IdFeature => true
      case _ => false
    }).head :: dimension.linkedConcepts.toList.flatMap(c => dimensionFeaturesSorted(c._2))

  def generateDataWrappers(nWrap: Int, counter: Iterator[Int], levels: List[List[Feature]],measure: (Edge,Measure)): List[Wrapper] = {
    def crossJoin[T](list: List[List[T]]): List[List[T]] =
      list match {
        case xs :: Nil => xs map (List(_))
        case x :: xs => for {
          i <- x
          j <- crossJoin(xs)
        } yield List(i) ++ j
      }
    val wrapperCombinations = crossJoin(levels).take(5)
    println("#Wrapper DATA:" + wrapperCombinations.size)
    wrapperCombinations.map(x => {
      val c = counter.next()
      val attributes = {
        Attribute(measure._2.name + c) sameAs measure._2
      }::x.map(feat => Attribute(feat.name + c) sameAs feat)
      WrapperImpl("WrapperW" + c,attributes.toSet)
    })
  }

  def generateLutWrappers(counter: Iterator[Int],levels: List[List[Feature]]): List[Wrapper] = {
    def generateWrapperForDimension(dimFeat: List[Feature]):List[Wrapper] = dimFeat match {
      case h1 :: h2 :: t =>
        val c = counter.next()
        val w =
          Wrapper("LUT" + c)
            .hasAttribute(Attribute(h1.name + "_lut_" + c) sameAs h1)
            .hasAttribute(Attribute(h2.name + "_lut_" + c) sameAs h2)
        w :: generateWrapperForDimension(h2 :: t)
      case _ :: Nil => Nil
    }
    val w = levels.flatMap(generateWrapperForDimension)
    println("Wrapper LUT:" + w.size)
    w
  }

  def generateWrappers(nWrappers: Int,levels: List[List[Feature]], measure: (Edge,Measure)): Set[Wrapper] = {
    val it = new Iterator[Int] {
      var c = 0
      override def hasNext: Boolean = true

      override def next(): Int = {c = c + 1;c}
    }
    generateDataWrappers(nWrappers,it,levels,measure).toSet ++ generateLutWrappers(it,levels)
  }

  def generateExperimentsIntegrationGraph(nDimensions: Int, nLevels: Int, nWrappers: Int): (Concept,Concept,Set[Wrapper],AggregatingFunction) = {
    val dimensions = generateDimensions(nDimensions,nLevels)
    val measure = (Edge("hasFeature"),Measure("M"))
    val aggregatingFunction = AggregatingFunction("sum") aggregates measure._2
    val experimentGraph = GenericConcept("EXPERIMENT",dimensions,Set(measure))
    val wrappers = generateWrappers(nWrappers,dimensions.map(c => dimensionFeaturesSorted(c._2)).toList,measure)
    val query = GenericConcept("EXPERIMENT",generateDimensionsQ(nDimensions,nLevels),Set(measure))
    (experimentGraph,query,wrappers,aggregatingFunction)
  }

}

object TestGraphFact extends App {
  val ex = GraphFactory.generateExperimentsIntegrationGraph(1,5,1)
  ex._3.foreach(println)
}