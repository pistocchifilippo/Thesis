package implicitAggregation.model

import java.io.BufferedWriter

// Model definition

trait GraphComponent {
  def name:String
  def stringify(): String
}
case class Edge(name:String) extends GraphComponent {
  override def stringify(): String = ""
}

trait Feature extends GraphComponent {
  override def stringify(): String = s"s:${name} rdf:type G:Feature\n"
}
case class IdFeature(name:String) extends Feature {
  override def stringify(): String =
    super.stringify() + s"s:${name} rdfs:subClassOf sc:identifier\n"
}
case class GenericFeature(name:String) extends Feature
case class Measure(name: String) extends Feature

trait Concept extends GraphComponent {
  def linkedConcepts:Set[(Edge, Concept)]
  def linkedFeatures:Set[(Edge, Feature)]

  def hasFeature(feature: Feature):Concept
  def hasConcept(label:String)(concept: Concept):Concept
  def ->(label:String)(concept: Concept):Concept
  def ->(graphComponent: Feature):Concept

  def partOf(concept: Concept):Concept

  override def stringify(): String =
    s"s:${name} rdf:type G:Concept\n" +
      linkedFeatures.map(e => s"s:${name} G:${e._1.name} s:${e._2.name}\n").foldRight("")(_ + _) +
      linkedConcepts.map(e => s"s:${name} s:${e._1.name} s:${e._2.name}\n").foldRight("")(_ + _) +
      linkedFeatures.map(e => e._2.stringify()).foldRight("")(_ + _) +
      linkedConcepts.map(e => e._2.stringify()).foldRight("")(_ + _)
}

trait Graph extends Set[Concept] {
  // need a traverse that seems to traverse one Concept but actually traverses a Set of Concept
}

trait BaseConcept extends Concept {
  override def hasFeature(feature: Feature): Concept = this match {
    case GenericConcept(name,linkedConcepts,linkedFeatures) => GenericConcept(name,linkedConcepts,linkedFeatures + Tuple2(Edge("hasFeature"),feature))
    case Level(name,linkedConcepts,linkedFeatures) => Level(name,linkedConcepts,linkedFeatures + Tuple2(Edge("hasFeature"),feature))
  }


  override def hasConcept(label: String)(concept: Concept): Concept = this match {
    case GenericConcept(name,linkedConcepts,linkedFeatures) => GenericConcept(name,linkedConcepts + Tuple2(Edge(label),concept),linkedFeatures)
    case Level(name,linkedConcepts,linkedFeatures) => Level(name,linkedConcepts + Tuple2(Edge(label),concept),linkedFeatures)
  }

  override def ->(feature: Feature): Concept = hasFeature(feature)

  override def ->(label: String)(concept: Concept): Concept = hasConcept(label)(concept)

  override def partOf(concept: Concept): Concept = hasConcept("partOf")(concept)

}

case class GenericConcept(
                        name:String,linkedConcepts: Set[(Edge, Concept)],linkedFeatures: Set[(Edge, Feature)]
                      ) extends BaseConcept

case class Level(
                  name:String,linkedConcepts: Set[(Edge, Concept)],linkedFeatures: Set[(Edge, Feature)]
                ) extends BaseConcept {
  override def hasConcept(label: String)(concept: Concept): Concept =
    if (label != "partOf") throw new Exception("For a level the labels should be partOf") else super.hasConcept(label)(concept)

//  override def stringify(): String =
//    super.stringify() +
//      s"s:${name} rdf:type G:Level\n"
}

case class Fact(
                 name:String,linkedConcepts: Set[(Edge, Concept)],linkedFeatures: Set[(Edge, Feature)]
               ) extends BaseConcept

trait AggregatingFunction extends BaseConcept {
  def measures: Set[Measure]
  def aggregates(measure: Measure): AggregatingFunction

  override def hasConcept(label: String)(concept: Concept): Concept = this

  override def hasFeature(feature: Feature): Concept = this

  override def stringify(): String =
//    s"s:${name} rdf:type G:Function\n" +
      measures.map(m => s"s:${name} s:aggregates s:${m.name}\n").foldRight("")(_ + _)
}

case class Function(
                     name:String,measures: Set[Measure],linkedConcepts: Set[(Edge, Concept)],linkedFeatures: Set[(Edge, Feature)]
                   ) extends AggregatingFunction {
  override def aggregates(measure: Measure): AggregatingFunction = Function(name,measures + measure,Set.empty,Set.empty)
}

trait Attribute extends GraphComponent {
  def sameAs:Option[Feature]
  def sameAs(feature: Feature):Attribute
}

case class AttributeImpl (name: String, sameAs: Option[Feature]) extends Attribute {
  override def sameAs(feature: Feature): Attribute = AttributeImpl(this.name,Some(feature))

  override def stringify(): String =
    s"s:${name} rdf:type S:Attribute\n" + {
      sameAs match {
        case Some(feature) => s"s:${name} owl:sameAs s:${feature.name}\n"
        case None => ""
      }
    }
}

trait Wrapper extends GraphComponent {
  def features: Set[Feature]
  def attributes: Set[Attribute]
  def hasAttribute(attribute: Attribute):Wrapper
  def -> (attribute: Attribute):Wrapper
}

case class WrapperImpl(name: String,attributes: Set[Attribute]) extends Wrapper {
  override def hasAttribute(attribute: Attribute): Wrapper = WrapperImpl(this.name, this.attributes + attribute)
  override def ->(attribute: Attribute): Wrapper = hasAttribute(attribute)
  override def stringify(): String =
    s"s:${name} rdf:type S:Wrapper\n" +
      attributes.map(a => s"s:${name} S:hasAttribute s:${a.name}\n").foldRight("")(_ + _) +
      attributes.map(_.stringify()).foldRight("")(_ + _)

  override def features: Set[Feature] = attributes.map(_.sameAs).filter(_.nonEmpty).map(_.get)
}

// Operations definition

object Feature {
  def apply(name:String): Feature = GenericFeature(name)
}

object AggregatingFunction {
  def apply(name: String): AggregatingFunction =
    Function(name, Set.empty,Set.empty, Set.empty)
}

object Concept {

  def apply(name:String): Concept = GenericConcept(name,Set.empty,Set.empty)

  def linkedConceptsWithLabel(concept: Concept)(label: String):Set[Concept] =
    concept.linkedConcepts.filter(_._1.name == label).map(_._2)

  def linkedFeatureName(concept: Concept)(name: String):Feature =
    concept.linkedFeatures.filter(_._2.name == name).map(_._2).head

  def copyConcept(concept: Concept)(linkedConcepts: Set[(Edge,Concept)])(linkedFeatures: Set[(Edge,Feature)]): Concept = concept match {
    case GenericConcept(name,_,_) => GenericConcept(name,linkedConcepts,linkedFeatures)
    case Level(name,_,_) => Level(name,linkedConcepts,linkedFeatures)
  }

}

object Level {
  def apply(name: String): Level = Level(name,Set.empty,Set.empty)
}

object Fact {
  def apply(name: String): Fact = Fact(name,Set.empty,Set.empty)
}

object Attribute {
  def apply(name: String): Attribute = AttributeImpl(name,None)
}

object Wrapper {

  private type WrapperWriter = (Wrapper,BufferedWriter,String) => Unit

  object GenerateMappingsFile extends WrapperWriter {
    override def apply(wrapper: Wrapper, f: BufferedWriter, path:String): Unit = {
      f.write("s:" + wrapper.name + "-\n")
    }
  }

  object GenerateCsv extends WrapperWriter {
    override def apply(wrapper: Wrapper, f: BufferedWriter, path:String): Unit = {
      wrapper.attributes.foreach(a => f.write(a.name + ","))
    }
  }

  object GenerateWrapperFile extends WrapperWriter {
    override def apply(wrapper: Wrapper, f: BufferedWriter, path:String): Unit = {
      f.write("https://serginf.github.io/" + wrapper.name + "," + path + wrapper.name + ".csv\n")
    }
  }

  def apply(name:String): Wrapper = WrapperImpl("Wrapper" + name,Set.empty)

}

object Graph {
  def allConcept(graph: Concept): Set[Concept] =
    if(graph.linkedConcepts.isEmpty) Set(graph) else graph.linkedConcepts.flatMap(c => allConcept(c._2)) + graph

  def allLevels(graph: Concept): Set[Level] =
    graph match {
      case l: Level => graph.linkedConcepts.flatMap(c => allLevels(c._2)) + l
      case _ => graph.linkedConcepts.flatMap(c => allLevels(c._2))
    }

  def allFeatures(graph: Concept): Set[Feature] =
    graph.linkedFeatures.map(_._2) ++ graph.linkedConcepts.flatMap(c => allFeatures(c._2))

  def allMeasures(graph: Concept): Set[Measure] =
    graph.linkedFeatures.collect(f => f._2 match {
      case m:Measure => m
    }) ++ graph.linkedConcepts.flatMap(c => allMeasures(c._2))

  def getIdFromConcept(concept: Concept): scala.collection.immutable.Set[IdFeature] = concept.linkedFeatures.map(_._2).collect({
    case f:IdFeature => f
  })
  
  def copyAllGraph(graph: Concept): Concept = {
    Concept.copyConcept(graph)(graph.linkedConcepts.map(edge => (edge._1,copyAllGraph(edge._2))))(graph.linkedFeatures)
  }
}