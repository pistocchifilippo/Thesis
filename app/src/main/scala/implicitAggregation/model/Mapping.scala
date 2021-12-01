package implicitAggregation.model

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Scanner

object Mapping {

  def mapping(features:Set[Feature], graph: Concept, conceptPath: Set[String]):Set[String] = {
    val rdfTypeConcept = Set(s"s:${graph.name} rdf:type G:Concept")
    val featureMappings = {
      if (graph.linkedFeatures.map(f => features.contains(f._2)).foldRight(false)(_ || _)) {
        graph.linkedFeatures.filter(f => features.contains(f._2)).flatMap(f =>
          f._2 match {
            case IdFeature(name) => Set(s"s:${graph.name} G:hasFeature s:$name", s"s:$name rdfs:subClassOf sc:identifier", s"s:$name rdf:type G:Feature")
            case GenericFeature(name) => Set(s"s:${graph.name} G:hasFeature s:$name", s"s:$name rdf:type G:Feature")
            case Measure(name) => Set(s"s:${graph.name} G:hasFeature s:$name", s"s:$name rdf:type G:Feature")
          }
        )
      } else {
        Set.empty
      }
    }
    val aggConceptPath =
      if (featureMappings.nonEmpty){
        conceptPath
      } else {
        Set.empty
      }

    val conceptMappings =
      if (graph.linkedConcepts.nonEmpty) {
        graph.linkedConcepts.flatMap(c => mapping(features, c._2,
          if (conceptPath.isEmpty && featureMappings.isEmpty){
            Set.empty
          } else if (aggConceptPath.nonEmpty) {
            Set(s"s:${graph.name} s:${c._1.name} s:${c._2.name}")
          } else {
            conceptPath + s"s:${graph.name} s:${c._1.name} s:${c._2.name}"
          }
        ))
      } else {
        Set.empty
      }
    featureMappings ++ conceptMappings ++ aggConceptPath ++ rdfTypeConcept
  }

  def intoLines(mapping: Set[String])(globalGraphPath: String): List[Int] = {
    val f = new Scanner(new File(globalGraphPath))
    var c = 1
    var mappings: List[Int] = List.empty
    while(f.hasNext){
      val l = f.nextLine()
//      println(l)
//      println(mapping)
      if(mapping.contains(l)) {
//        println("YESs")
        mappings = mappings :+ c
      }
      c = c + 1
    }
    mappings
  }

  def generateMappingFile(wrappers: Set[Wrapper])(graph: Concept)(globalGraphPath: String)(f: BufferedWriter): Unit = {
    wrappers.foreach(w => f.write(
      s"s:${w.name}-${
        intoLines(mapping(w.features,graph,Set.empty))(globalGraphPath).mkString(",")
      }\n"
    ))
  }

}

object Test extends App {

  val CITY = IdFeature("CITY")
  val REGION = IdFeature("REGION")
  val COUNTRY = IdFeature("COUNTRY")
  val REVENUE = GenericFeature("REVENUE")

  // Graph hierarchy
  val Sales =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
        Concept("City")
          .hasFeature{CITY}
          .partOf{
            Concept("Region")
              .hasFeature{REGION}
              .partOf{
                Concept("Country")
                  .hasFeature{COUNTRY}
              }
          }
      }

  // Wrappers
  val w1 =
    Wrapper("W1")
      .hasAttribute{Attribute("country") sameAs COUNTRY}
      .hasAttribute{Attribute("revenue") sameAs REVENUE}

  val w2 =
    Wrapper("W2")
      .hasAttribute{Attribute("region") sameAs REGION}
      .hasAttribute{Attribute("revenue2") sameAs REVENUE}

  val w3 =
    Wrapper("LUT")
      .hasAttribute{Attribute("region1") sameAs REGION}
      .hasAttribute{Attribute("country1") sameAs COUNTRY}

  val f = new BufferedWriter(new FileWriter("scenarios/LUT/mappings.txt"))
  Mapping.generateMappingFile(Set(w1,w2,w3))(Sales)("scenarios/LUT/global_graph.txt")(f)
  f.close()

}
