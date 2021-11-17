package implicitAggregation.scenarios

import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, GenericFeature, IdFeature, Level, Measure, QueryExecution, Utils, Wrapper}

object BugNullValues extends App {

  val scenario = "BugNullValues"

  // GRAPH DEFINITION

  // Features
  val PID = IdFeature("pId")
  val NAME = GenericFeature("Name")
  val SURNAME = GenericFeature("Surname")
  val INCOME = GenericFeature("Income")
  val LIVING_ADDRESS = GenericFeature("LivingAddress")

  // Graph hierarchy
  val graph =
    Concept("Person")
      .hasFeature{PID}
      .hasFeature{NAME}
      .hasFeature{SURNAME}
      .hasFeature{INCOME}
      .hasFeature{LIVING_ADDRESS}

  // The query
  val query =
    Concept("Person")
      .hasFeature{PID}
      .hasFeature{NAME}
      .hasFeature{SURNAME}
      .hasFeature{INCOME}


  // Wrappers
  val w1 =
    Wrapper("W1")
      .hasAttribute{Attribute("pId") sameAs PID}
      .hasAttribute{Attribute("Name") sameAs NAME}
      .hasAttribute{Attribute("Surname") sameAs SURNAME}
//      .hasAttribute{Attribute("Income") sameAs INCOME} WITH THIS LINE WORKS

  // WRITERS
  Utils.generateAllFiles(Set(graph),Set(w1),query)(scenario)
  QueryExecution.execute(scenario,Utils.SCENARIOS_PATH,makeImplicitAggregation = false)(query)(Set.empty)(Set(w1))

}
