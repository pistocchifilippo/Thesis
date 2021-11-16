package implicitAggregation.model

import java.util.Scanner

object Query {

  def generateQuery(graph: Concept): String = {
    s"#q1\nSELECT ${select(graph)}\nWHERE {\n  ${values(graph)}\n  ${clean(graph.stringify()).replace("\n"," .\n  ")}\n}\n#"
  }
  def allFeature(graph: Concept): Set[Feature] =
    graph.linkedFeatures.map(_._2) ++ {if (graph.linkedConcepts.nonEmpty) graph.linkedConcepts.flatMap(c => allFeature(c._2)) else Set.empty}

  def select(graph: Concept): String = s"${allFeature(graph).map(f => s"?${f.name} ").foldRight("")(_ + _)}"
  def values(graph: Concept): String =
    s"VALUES ( ${select(graph)}) { ( ${allFeature(graph).map(f => s"s:${f.name} ").foldRight("")(_ + _)}) }"

  def clean(source: String): String = {
    var out = ""
    val s = new Scanner(source)
    while (s.hasNext()){
      val l= s.nextLine()
      if(!l.contains("G:Concept") && !l.contains("G:Feature") && !l.contains("sc:identifier") && !l.contains("G:Measure")) {
        out = out + l + "\n"
      }
    }
    out
  }
}

object TestQ extends App {

  val CITY = IdFeature("CITY")
  val REGION = IdFeature("REGION")
  val COUNTRY = IdFeature("COUNTRY")
  val REVENUE = GenericFeature("REVENUE")

  // Graph hierarchy
  val Sales =
    Concept("Sales")
      .hasFeature {
        REVENUE
      }
      .->("location") {
        Concept("City")
          .hasFeature {
            CITY
          }
          .partOf {
            Concept("Region")
              .hasFeature {
                REGION
              }
              .partOf {
                Concept("Country")
                  .hasFeature {
                    COUNTRY
                  }
              }
          }
      }


  println(Query.generateQuery(Sales))


}